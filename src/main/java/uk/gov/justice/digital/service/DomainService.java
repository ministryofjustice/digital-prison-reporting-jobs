package uk.gov.justice.digital.service;

import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    public void refreshDomainUsingDataFrame(SparkSession spark, Dataset<Row> dataFrame, Row tableInfo) {
        String tableName = tableInfo.getAs(TABLE);
        val domainDefinitions = getDomainDefinitions();
        val domainDefinitionsForSource = filterDomainDefinitionsForSource(domainDefinitions, tableName);
        logger.info("Found {} domains with source table {}", domainDefinitionsForSource.size(), tableName);

        if (domainDefinitionsForSource.isEmpty()) {
            // log and proceed when no domain is found for the table
            logger.warn("No domain definition found for table: " + tableName);
        } else {
            domainDefinitionsForSource.forEach(domainDefinition ->
                    dataFrame.collectAsList()
                            .forEach(row -> refreshCDCRecord(spark, domainDefinition, row, tableName))
            );
        }
    }

    private void refreshCDCRecord(SparkSession spark, DomainDefinition domainDefinition, Row row, String recordTableName) {
        String unvalidatedOperation = row.getAs(OPERATION);
        val domainName = domainDefinition.getName();
        val optionalOperation = getOperation(unvalidatedOperation);
        val cdcOperations = optionalOperation.filter(operation -> operation != Load); // discard load records

        cdcOperations.ifPresent(operation -> {
            val singleRecord = spark.createDataFrame(new ArrayList<>(ImmutableList.of(row)), row.schema());

            domainDefinition.getTables().forEach(tableDefinition -> {
                        val tableTransform = tableDefinition.getTransform();
                        val violations = tableDefinition.getViolations();
                        val sourceToRecordMap = buildSourceToRecordsMap(singleRecord, tableDefinition, recordTableName);

                        try {
                            String tableName = tableDefinition.getName();

                            logger.info("Applying transform for CDC record of table " + tableName);
                            val transformedDataFrame = executor.applyTransform(spark, sourceToRecordMap, tableTransform);

                            logger.info("Applying violations for CDC record of table " + tableName);
                            executor.applyViolations(spark, transformedDataFrame, violations);

                            logger.info("Saving transformed CDC record of table " + tableName);
                            executor.saveDomain(spark, transformedDataFrame, domainName, tableDefinition, operation);
                        } catch (Exception e) {
                            logger.warn(
                                    "Failed to incrementally {} domain {} and table {}",
                                    operation,
                                    domainName,
                                    recordTableName,
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
            logger.info("Retrieved domain definition for domain: '{}' table: '{}'", domainName, tableName);
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
            Dataset<Row> dataFrame,
            TableDefinition tableDefinition,
            String rowTable
    ) {
        Map<String, Dataset<Row>> sourceToRecordMap = new HashMap<>();
        tableDefinition
                .getTransform()
                .getSources()
                .forEach(source -> {
                    if (source.toLowerCase().endsWith(rowTable.toLowerCase())) {
                        sourceToRecordMap.put(source, dataFrame);
                    }
                });
        return sourceToRecordMap;
    }
}
