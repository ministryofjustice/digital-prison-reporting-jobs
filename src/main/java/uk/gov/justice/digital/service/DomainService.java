package uk.gov.justice.digital.service;

import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;
import lombok.val;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.dynamodb.DomainDefinitionClient;
import uk.gov.justice.digital.common.SourceMapping;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.DomainExecutor;
import uk.gov.justice.digital.domain.ViewTextProcessor;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.domain.model.TableDefinition;
import uk.gov.justice.digital.exception.DatabaseClientException;
import uk.gov.justice.digital.exception.DomainExecutorException;
import uk.gov.justice.digital.exception.DomainServiceException;

import javax.inject.Singleton;
import java.util.*;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.Operation.Load;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.Operation.getOperation;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.*;

@Singleton
public class DomainService {

    private static final Logger logger = LoggerFactory.getLogger(DomainService.class);

    private final DomainDefinitionClient domainClient;
    private final DomainExecutor executor;
    private final JobArguments arguments;

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

    public void refreshDomainUsingDataFrame(SparkSession spark, Dataset<Row> dataFrame, String sourceName, String tableName) {
        val sourceTable = sourceName + "." + tableName;
        val domainDefinitions = getDomainDefinitions();
        val domainDefinitionsForSource = filterDomainDefinitionsForSource(domainDefinitions, sourceTable);
        logger.debug("Found {} domains with source table {}", domainDefinitionsForSource.size(), sourceTable);

        if (domainDefinitionsForSource.isEmpty()) {
            // log and proceed when no domain is found for the table
            logger.warn("No domain definition found for table: " + sourceTable);
        } else {
            domainDefinitionsForSource.forEach(domainDefinition ->
                    dataFrame.collectAsList()
                            .forEach(row -> refreshDomainUsingCDCRecord(spark, domainDefinition, row, sourceTable))
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
                                                .anyMatch(source -> source.equalsIgnoreCase(sourceName)))
                ).collect(Collectors.toList());
    }

    private Map<String, Dataset<Row>> buildSourceToRecordsMap(
            SparkSession spark,
            Dataset<Row> referenceDataFrame,
            TableDefinition tableDefinition,
            String referenceTableName
    ) {
        val sourceToRecordMap = new HashMap<String, Dataset<Row>>();
        val sourcesWithoutRecords = new HashSet<String>();

        val viewText = tableDefinition.getTransform().getViewText().replaceAll("\\n", " ");
        val sourceMappings = ViewTextProcessor.buildAllSourceMappings(tableDefinition.getTransform().getSources(), viewText);

        tableDefinition.getTransform().getSources().forEach(source -> {
                    if (source.equalsIgnoreCase(referenceTableName)) {
                        sourceToRecordMap.put(source, referenceDataFrame);
                    } else {
                        val optionalSourceMapping = findRelatedSourceMapping(referenceTableName, sourceMappings, source);
                        if (optionalSourceMapping.isPresent()) {
                            val adjoiningDataFrame = executor.getAdjoiningDataFrame(spark, optionalSourceMapping.get(), referenceDataFrame);
                            if (adjoiningDataFrame.isEmpty()) {
                                logger.warn("Adjoining dataFrame is empty for source {} given {}", source, referenceTableName);
                            } else {
                                logger.debug("Adding dataframe for source {} and ref: {}", source, referenceTableName);
                                sourceToRecordMap.put(source, adjoiningDataFrame);
                            }
                        } else {
                            sourcesWithoutRecords.add(source);
                        }
                    }
                }
        );

        val missingSourceRecords = getMissingSourceRecords(spark, sourcesWithoutRecords, sourceMappings, sourceToRecordMap);
        sourceToRecordMap.putAll(missingSourceRecords);

        return sourceToRecordMap;
    }

    private Map<String, Dataset<Row>> getMissingSourceRecords(
            SparkSession spark,
            Set<String> sourcesWithoutDataframes,
            Set<SourceMapping> sourceMappings,
            Map<String, Dataset<Row>> sourceToRecordMap
    ) {
        val missingSourceRecords = new HashMap<String, Dataset<Row>>();

        sourcesWithoutDataframes.forEach(source ->
                sourceMappings
                        .stream()
                        .filter(sourceMapping -> sourceMapping.getDestinationTable().equalsIgnoreCase(source) &&
                                sourceToRecordMap.containsKey(sourceMapping.getSourceTable()))
                        .findFirst()
                        .ifPresent(sourceMapping -> {
                            val reverseMapping = sourceMapping.withSourceColumnsUpperCased();
                            val referenceDataFrame = sourceToRecordMap.get(sourceMapping.getSourceTable());
                            val adjoiningDataFrame = executor.getAdjoiningDataFrame(spark, reverseMapping, referenceDataFrame);
                            if (adjoiningDataFrame.isEmpty()) {
                                logger.warn("Failed to retrieve dataFrame for {} using already retrieved source {}", source, sourceMapping.getSourceTable());
                            } else {
                                logger.debug("Missing record retrieved for source: {}", source);
                                missingSourceRecords.put(source, adjoiningDataFrame);
                            }
                        }));

        return missingSourceRecords;
    }

    @NotNull
    private static Optional<SourceMapping> findRelatedSourceMapping(
            String source,
            Set<SourceMapping> sourceMappings,
            String destination
    ) {
        return sourceMappings
                .stream()
                .filter(sourceMapping -> sourceMapping.getSourceTable().equalsIgnoreCase(source) &&
                        sourceMapping.getDestinationTable().equalsIgnoreCase(destination))
                .findFirst();
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
