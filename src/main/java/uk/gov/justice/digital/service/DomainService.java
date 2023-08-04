package uk.gov.justice.digital.service;


import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.dynamodb.DomainDefinitionClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datamart.DataMartMapper;
import uk.gov.justice.digital.domain.DomainExecutor;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DatabaseClientException;
import uk.gov.justice.digital.exception.DomainExecutorException;
import uk.gov.justice.digital.exception.DomainServiceException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Stream;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.Operation.Load;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.Operation.getOperation;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.*;

@Singleton
public class DomainService {

    private static final Logger logger = LoggerFactory.getLogger(DomainService.class);

    private final DomainDefinitionClient domainClient;
    private final DomainExecutor executor;
//    private final DataMartMapper dataMartMapper;
    private final JobArguments arguments;

    @Inject
    public DomainService(JobArguments arguments,
                         DomainDefinitionClient domainClient,
                         DomainExecutor executor
                         ) {
        this.arguments = arguments;
        this.domainClient = domainClient;
        this.executor = executor;
//        this.dataMartMapper = dataMartMapper;
    }

    public void run() throws DomainServiceException, DomainExecutorException {
        runInternal(
                arguments.getDomainTableName(),
                arguments.getDomainName(),
                arguments.getDomainOperation()
        );
    }

    public void processIncrementally(SparkSession spark, Dataset<Row> dataFrame, Row table) {

//        String rowTable = table.getAs(TABLE);
//        val relevantDomainDefinitions = getRelevantDomainDefinitions(spark, rowTable);
//
//        relevantDomainDefinitions.forEach(domainDefinition ->
//                dataFrame.collectAsList().forEach(row -> {
//                            String unvalidatedOperation = row.getAs(OPERATION);
//                            val optionalOperation = getOperation(unvalidatedOperation);
//
//                            optionalOperation.filter(operation -> operation != Load).ifPresent(operation -> {
//                                val records = spark.createDataFrame(new ArrayList<>(ImmutableList.of(row)), row.schema());
//
//                                domainDefinition.getTables().forEach(tableDefinition -> {
//                                            Map<String, Dataset<Row>> refs = new HashMap<>();
//                                            tableDefinition.getTransform().getSources().forEach(source -> refs.put(source, records));
//                                            try {
//                                                val transformedDataFrame = executor.applyTransform(refs, tableDefinition.getTransform());
//
//                                                executor.applyViolations(transformedDataFrame, tableDefinition.getViolations());
//
//                                                dataMartMapper.mapToRedshift(
//                                                        transformedDataFrame,
//                                                        domainDefinition.getName(),
//                                                        tableDefinition,
//                                                        row.getAs(tableDefinition.getPrimaryKey()),
//                                                        operation
//                                                );
//                                            } catch (DomainExecutorException | DataStorageException e) {
//                                                logger.error("Failed to process domain for record", e);
//                                            }
//                                        }
//                                );
//                            });
//                        }
//                )
//        );
    }

    private void runInternal(
            String domainTableName,
            String domainName,
            String domainOperation
    ) throws PatternSyntaxException, DomainServiceException, DomainExecutorException {
        if (domainOperation.equalsIgnoreCase("delete"))
            executor.doDomainDelete(domainName, domainTableName);
        else {
            val domain = getDomainDefinition(domainName, domainTableName);
            processDomain(domain, domain.getName(), domainTableName, domainOperation);
        }
    }

    private void processDomain(
            DomainDefinition domain,
            String domainName,
            String domainTableName,
            String domainOperation
    ) throws DomainExecutorException {
        val prefix = "processing of domain: '" + domainName + "' operation: " + domainOperation + " ";

        logger.info(prefix + "started");
        executor.doFullDomainRefresh(domain, domainTableName, domainOperation);
        logger.info(prefix + "completed");
    }

    private DomainDefinition getDomainDefinition(String domainName,
                                                 String tableName) throws DomainServiceException {
        try {
            val domainDefinition = domainClient.getDomainDefinition(domainName, tableName);
            logger.info("Retrieved domain definition for domain: '{}' table: '{}'", domainName, tableName);
            return domainDefinition;
        } catch (DatabaseClientException e) {
            logger.error("DynamoDB request failed: ", e);
            throw new DomainServiceException("DynamoDB request failed: ", e);
        }
    }

    @NotNull
    private Stream<DomainDefinition> getRelevantDomainDefinitions(SparkSession spark, String rowTable) {
        return executor.getDomainDefinitions(spark, arguments.getDomainRegistry())
                .stream()
                .filter(domainDefinition -> domainDefinition
                        .getTables()
                        .stream()
                        .anyMatch(tableDefinition -> tableDefinition
                                .getTransform()
                                .getSources()
                                .stream()
                                .anyMatch(source -> source.toLowerCase().endsWith(rowTable.toLowerCase()))
                        )
                );
    }
}
