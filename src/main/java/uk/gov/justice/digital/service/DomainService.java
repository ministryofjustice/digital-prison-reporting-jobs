package uk.gov.justice.digital.service;

import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;
import lombok.val;
import org.apache.hadoop.shaded.com.google.re2j.Pattern;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.constructor.DuplicateKeyException;
import uk.gov.justice.digital.client.dynamodb.DomainDefinitionClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.DomainExecutor;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.domain.model.TableDefinition;
import uk.gov.justice.digital.exception.DatabaseClientException;
import uk.gov.justice.digital.exception.DomainExecutorException;
import uk.gov.justice.digital.exception.DomainServiceException;

import javax.inject.Singleton;
import java.util.*;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.Operation.Load;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.Operation.getOperation;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.*;

@Singleton
public class DomainService {

    private static final Logger logger = LoggerFactory.getLogger(DomainService.class);

    private final DomainDefinitionClient domainClient;
    private final DomainExecutor executor;
    private final JobArguments arguments;
    private static final Pattern aliasRegexPattern = Pattern.compile(" (\\S+)(\\s+as\\s+)(\\S+) ");
    private static final Pattern joinExprRegexPattern = Pattern
            .compile("((join)|((left)(\\s+)(join))|((right)(\\s+)(join))|((full)(\\s+)(join)))");

    @Inject
    public DomainService(
            JobArguments arguments,
            DomainDefinitionClient domainClient,
            DomainExecutor executor
    ) {
        logger.info("Initializing DomainService");
        this.arguments = arguments;
        this.domainClient = domainClient;
        this.executor = executor;
        logger.info("DomainService initialization complete");
    }

    public void run(SparkSession spark) throws DomainServiceException, DomainExecutorException {
        runInternal(
                spark,
                arguments.getDomainTableName(),
                arguments.getDomainName(),
                arguments.getDomainOperation()
        );
    }

    public void refreshDomainUsingDataFrame(SparkSession spark, Dataset<Row> dataFrame, Row tableInfo) {
        String tableName = tableInfo.getAs(TABLE);
        val domainDefinitions = getDomainDefinitions();
        val domainDefinitionsForSource = filterDomainDefinitionsForSource(domainDefinitions, tableName);
        logger.debug("Found {} domains with source table {}", domainDefinitionsForSource.size(), tableName);

        if (domainDefinitionsForSource.isEmpty()) {
            // log and proceed when no domain is found for the table
            logger.warn("No domain definition found for table: " + tableName);
        } else {
            domainDefinitionsForSource.forEach(domainDefinition ->
                    dataFrame.collectAsList()
                            .forEach(row -> refreshDomainUsingCDCRecord(spark, domainDefinition, row, tableName))
            );
        }
    }

    private void refreshDomainUsingCDCRecord(SparkSession spark, DomainDefinition domainDefinition, Row row, String referenceTableName) {
        String unvalidatedOperation = row.getAs(OPERATION);
        val domainName = domainDefinition.getName();
        val optionalOperation = getOperation(unvalidatedOperation);
        val cdcOperations = optionalOperation.filter(operation -> operation != Load); // discard load records

        cdcOperations.ifPresent(operation -> {
            val referenceDataframe = lowerCaseColumnNames(spark, row);

            domainDefinition.getTables().forEach(tableDefinition -> {
                        val tableTransform = tableDefinition.getTransform();
                        val violations = tableDefinition.getViolations();

                        try {
                            String tableName = tableDefinition.getName();
                            val sourceToRecordMap = buildSourceToRecordsMap(spark, referenceDataframe, tableDefinition, referenceTableName);

                            logger.debug("Applying transform for CDC record of table " + tableName);
                            val transformedDataFrame = executor.applyTransform(spark, sourceToRecordMap, tableTransform);
                            logger.debug("Transformed record " + transformedDataFrame);

                            logger.debug("Applying violations for CDC record of table " + tableName);
                            executor.applyViolations(spark, transformedDataFrame, violations);

                            logger.debug("Saving transformed CDC record of table " + tableName);
                            executor.applyDomain(spark, transformedDataFrame, domainName, tableDefinition, operation);
                        } catch (Exception e) {
                            logger.warn(
                                    "Failed to incrementally {} domain {}_{} given record {}",
                                    operation,
                                    domainName,
                                    tableDefinition.getName(),
                                    referenceTableName,
                                    e
                            );
                        }
                    }
            );
        });
    }

    private void runInternal(
            SparkSession spark,
            String domainTableName,
            String domainName,
            String domainOperation
    ) throws PatternSyntaxException, DomainServiceException, DomainExecutorException {
        if (domainOperation.equalsIgnoreCase("delete"))
            executor.doDomainDelete(spark, domainName, domainTableName);
        else {
            val domain = getDomainDefinition(domainName, domainTableName);
            processDomain(spark, domain, domain.getName(), domainTableName, domainOperation);
        }
    }

    private void processDomain(
            SparkSession spark,
            DomainDefinition domain,
            String domainName,
            String domainTableName,
            String domainOperation
    ) throws DomainExecutorException {
        val prefix = "processing of domain: '" + domainName + "' operation: " + domainOperation + " ";

        logger.info(prefix + "started");
        executor.doFullDomainRefresh(spark, domain, domainTableName, domainOperation);
        logger.info(prefix + "completed");
    }

    private DomainDefinition getDomainDefinition(String domainName,
                                                 String tableName) throws DomainServiceException {
        try {
            val domainDefinition = domainClient.getDomainDefinition(domainName, tableName);
            logger.debug("Retrieved domain definition for domain: '{}' table: '{}'", domainName, tableName);
            return domainDefinition;
        } catch (DatabaseClientException e) {
            String errorMessage = "DynamoDB request failed: ";
            logger.error(errorMessage, e);
            throw new DomainServiceException(errorMessage, e);
        }
    }

    public List<DomainDefinition> getDomainDefinitions() {
        // An irrecoverable error is thrown when unable to retrieve domains or the domain registry is empty
        try {
            val domainDefinitions = domainClient.getDomainDefinitions();
            if (domainDefinitions.isEmpty()) {
                throw new DomainServiceException("No domain definition found");
            } else {
                return domainDefinitions;
            }
        } catch (DatabaseClientException | DomainServiceException e) {
            logger.error("Failed to get domain definitions: ", e);
            throw new RuntimeException(e);
        }
    }

    private List<DomainDefinition> filterDomainDefinitionsForSource(
            List<DomainDefinition> domainDefinitions,
            String sourceName
    ) {
        return domainDefinitions.stream()
                .filter(domain ->
                        domain.getTables()
                                .stream()
                                .anyMatch(table ->
                                        table.getTransform()
                                                .getSources()
                                                .stream()
                                                .anyMatch(source -> source.toLowerCase().endsWith("." + sourceName.toLowerCase())))
                ).collect(Collectors.toList());
    }

    private Map<String, Dataset<Row>> buildSourceToRecordsMap(
            SparkSession spark,
            Dataset<Row> referenceDataFrame,
            TableDefinition tableDefinition,
            String referenceTableName
    ) {
        val sourceToRecordMap = new HashMap<String, Dataset<Row>>();
        val viewText = tableDefinition.getTransform().getViewText().replaceAll("\\n", " ");
        tableDefinition
                .getTransform()
                .getSources()
                .forEach(adjoiningTableName -> {
                    if (adjoiningTableName.toLowerCase().endsWith(referenceTableName.toLowerCase())) {
                        sourceToRecordMap.put(adjoiningTableName, referenceDataFrame);
                    } else {
                        try {
                            val columnMappings = buildColumnMapBetweenReferenceAndAdjoiningTables(referenceTableName, adjoiningTableName, viewText);
                            val adjoiningDataFrame = executor
                                    .getAdjoiningDataFrameForReferenceDataFrame(spark, adjoiningTableName, columnMappings, referenceDataFrame);
                            if (adjoiningDataFrame.isEmpty()) {
                                logger.warn("Adjoining dataFrame is empty for source {} and viewText {}", adjoiningTableName, viewText);
                            } else {
                                sourceToRecordMap.put(adjoiningTableName, adjoiningDataFrame);
                            }
                        } catch (DomainServiceException | DuplicateKeyException e) {
                            logger.warn("Failed to to get dataFrame for other table source {} ", adjoiningTableName, e);
                        }
                    }
                });
        return sourceToRecordMap;
    }

    protected Map<String, String> buildColumnMapBetweenReferenceAndAdjoiningTables(
            String referenceTableName,
            String adjoiningTableName,
            String viewText
    ) throws DomainServiceException {
        val columnMappings = new HashMap<String, String>();

        val fromExpression = viewText.toLowerCase().split(" from ", 2)[1];
        val aliasesResolved = resolveAliases(fromExpression);
        val joinExpressions = aliasesResolved.split(joinExprRegexPattern.pattern());

        for (String joinExpression : joinExpressions) {
            val onExpressions = joinExpression.split(" on ");

            if (onExpressions.length == 2) {
                // ViewText has a join expression
                val joinClause = onExpressions[1];
                val andExpressions = Arrays
                        .stream(joinClause.split(" and "))
                        .map(String::trim)
                        .filter(x ->
                                x.toLowerCase().contains(referenceTableName.toLowerCase()) &&
                                        x.toLowerCase().contains(adjoiningTableName.toLowerCase())
                        )
                        .collect(Collectors.toList());

                val joinColumns = andExpressions
                        .stream()
                        .flatMap(str -> Arrays.stream(str.trim().split("=")).map(String::trim))
                        .collect(Collectors.toList());

                val currentTableJoinColumns = joinColumns
                        .stream()
                        .filter(column -> column.trim().contains(referenceTableName.toLowerCase()))
                        .map(x -> x.substring(x.lastIndexOf(".") + 1))
                        .collect(Collectors.toList());

                val otherTableJoinColumns = joinColumns
                        .stream()
                        .filter(column -> column.trim().contains(adjoiningTableName.toLowerCase()))
                        .map(x -> x.substring(x.lastIndexOf(".") + 1))
                        .collect(Collectors.toList());

                if (currentTableJoinColumns.size() == otherTableJoinColumns.size()) {
                    val intermediateMap = IntStream.range(0, currentTableJoinColumns.size())
                            .boxed()
                            .collect(Collectors.toMap(currentTableJoinColumns::get, otherTableJoinColumns::get));
                    columnMappings.putAll(intermediateMap);
                } else {
                    throw new DomainServiceException("Join expression length mismatch in viewText " + viewText);
                }
            }
        }

        return columnMappings;
    }

    private String resolveAliases(String sqlText) {
        String aliasReplacedText = sqlText;
        val matcher = aliasRegexPattern.matcher(sqlText);

        while (matcher.find()) {
            val originalName = matcher.group(1);
            val alias = matcher.group(3);
            aliasReplacedText = aliasReplacedText
                    .replaceAll(" as " + alias, " ")
                    .replaceAll(alias, originalName);
        }

        return aliasReplacedText;
    }

    private static Dataset<Row> lowerCaseColumnNames(SparkSession spark, Row row) {
        val dataFrame = spark.createDataFrame(new ArrayList<>(ImmutableList.of(row)), row.schema());
        return dataFrame.select(
                Arrays.stream(dataFrame.columns())
                        .map(column -> functions.col(column).as(column.toLowerCase()))
                        .toArray(Column[]::new)
        );
    }
}
