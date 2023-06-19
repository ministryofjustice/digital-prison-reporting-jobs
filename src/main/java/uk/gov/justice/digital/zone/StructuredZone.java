package uk.gov.justice.digital.zone;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.converter.dms.DMS_3_4_6;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.job.udf.JsonValidator;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;

import static org.apache.spark.sql.functions.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.Operation.*;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.*;

@Singleton
public class StructuredZone extends FilteredZone {

    public static final String ERROR = "error";
    public static final String PARSED_DATA = "parsedData";
    public static final String VALID = "valid";

    private static final Logger logger = LoggerFactory.getLogger(StructuredZone.class);

    private static final Map<String, String> jsonOptions = Collections.singletonMap("ignoreNullFields", "false");

    private final String structuredPath;
    private final String violationsPath;
    private final DataStorageService storage;
    private final SourceReferenceService sourceReferenceService;

    @Inject
    public StructuredZone(JobArguments jobArguments,
                          DataStorageService storage,
                          SourceReferenceService sourceReferenceService) {
        this.structuredPath = jobArguments.getStructuredS3Path();
        this.violationsPath = jobArguments.getViolationsS3Path();
        this.storage = storage;
        this.sourceReferenceService = sourceReferenceService;
    }

    @Override
    public Dataset<Row> processLoad(SparkSession spark, Dataset<Row> dataFrame, Row table) throws DataStorageException {
        return process(spark, dataFrame, table, false);
    }

    @Override
    public Dataset<Row> processCDC(SparkSession spark, Dataset<Row> dataFrame, Row table) throws DataStorageException {
        return process(spark, dataFrame, table, true);
    }

    private Dataset<Row> process(
            SparkSession spark,
            Dataset<Row> dataFrame,
            Row table,
            Boolean isCDC) throws DataStorageException {

        val filteredRecords = isCDC
                ? dataFrame.filter(col(OPERATION).isin(cdcOperations))
                : dataFrame.filter(col(OPERATION).equalTo(Load.getName()));

        val sortedRecords = filteredRecords.orderBy(col(TIMESTAMP));

        val rowCount = sortedRecords.count();

        logger.info("Processing batch with {} records", rowCount);

        val startTime = System.currentTimeMillis();

        String sourceName = table.getAs(SOURCE);
        String tableName = table.getAs(TABLE);

        val sourceReference = sourceReferenceService.getSourceReference(sourceName, tableName);

        logger.info("Processing {} records for {}/{}", rowCount, sourceName, tableName);

        val structuredDataFrame = sourceReference.isPresent()
                ? handleSchemaFound(spark, sortedRecords, sourceReference.get(), isCDC)
                : handleNoSchemaFound(spark, sortedRecords, sourceName, tableName);

        logger.info("Processed batch with {} rows in {}ms",
                rowCount,
                System.currentTimeMillis() - startTime
        );

        return structuredDataFrame;
    }

    private Dataset<Row> handleSchemaFound(SparkSession spark,
                                           Dataset<Row> dataFrame,
                                           SourceReference sourceReference,
                                           Boolean isCDC) throws DataStorageException {
        val tablePath = createValidatedPath(
                structuredPath,
                sourceReference.getSource(),
                sourceReference.getTable()
        );

        val validationFailedViolationPath = createValidatedPath(
                violationsPath,
                sourceReference.getSource(),
                sourceReference.getTable()
        );

        val validatedDataFrame = validateJsonData(
                spark,
                dataFrame,
                sourceReference.getSchema(),
                sourceReference.getSource(),
                sourceReference.getTable()
        );

        handleInvalidRecords(
                spark,
                validatedDataFrame,
                sourceReference.getSource(),
                sourceReference.getTable(),
                validationFailedViolationPath
        );

        return handleValidRecords(spark, validatedDataFrame, tablePath, sourceReference.getPrimaryKey(), isCDC);
    }

    private Dataset<Row> validateJsonData(SparkSession spark,
                                          Dataset<Row> dataFrame,
                                          StructType schema,
                                          String source,
                                          String table) {

        logger.info("Validating data against schema: {}/{}", source, table);

        val jsonValidator = JsonValidator.createAndRegister(schema, spark, source, table);

        return dataFrame
                .select(col(DATA), col(METADATA), col(OPERATION))
                .withColumn(PARSED_DATA, from_json(col(DATA), schema, jsonOptions))
                .withColumn(VALID, jsonValidator.apply(col(DATA), to_json(col(PARSED_DATA), jsonOptions)));
    }

    private Dataset<Row> handleValidRecords(SparkSession spark,
                                            Dataset<Row> dataFrame,
                                            String destinationPath,
                                            SourceReference.PrimaryKey primaryKey) throws DataStorageException {
        val validRecords = dataFrame
                .filter(col(VALID).equalTo(true))
                .select(PARSED_DATA + ".*", OPERATION);

        val validRecordsCount = validRecords.count();

        if (validRecordsCount > 0) {
            logger.info("Writing {} valid records", validRecordsCount);
            if (isCDC) {
                applyIncrementalRecordsAndUpdateManifest(spark, destinationPath, primaryKey, validRecords);
            } else {
                appendDataAndUpdateManifestForTable(spark, validRecords, destinationPath, primaryKey);
            }
            return validRecords;
        } else return createEmptyDataFrame(dataFrame);
    }

    private void handleInvalidRecords(SparkSession spark,
                                      Dataset<Row> dataFrame,
                                      String source,
                                      String table,
                                      String destinationPath) throws DataStorageException {

        val errorString = String.format("Record does not match schema %s/%s", source, table);

        // Write invalid records where schema validation failed
        val invalidRecords = dataFrame
                .select(col(DATA), col(METADATA), col(VALID))
                .filter(col(VALID).equalTo(false))
                .withColumn(ERROR, lit(errorString))
                .drop(col(VALID));

        val invalidRecordsCount = invalidRecords.count();

        if (invalidRecordsCount > 0) {
            logger.error("Structured Zone Violation - {} records failed schema validation", invalidRecordsCount);
            appendDataAndUpdateManifestForTable(spark, invalidRecords, destinationPath);
        }
    }

    private Dataset<Row> handleNoSchemaFound(SparkSession spark,
                                             Dataset<Row> dataFrame,
                                             String source,
                                             String table) throws DataStorageException {

        logger.error("Structured Zone Violation - No schema found for {}/{} - writing {} records",
                source,
                table,
                dataFrame.count()
        );

        val missingSchemaRecords = dataFrame
                .select(col(DATA), col(METADATA))
                .withColumn(ERROR, lit(String.format("Schema does not exist for %s/%s", source, table)));

        appendDataAndUpdateManifestForTable(
                spark,
                missingSchemaRecords,
                createValidatedPath(violationsPath, source, table)
        );
        return createEmptyDataFrame(dataFrame);
    }

    private void appendDataAndUpdateManifestForTable(SparkSession spark,
                                                     Dataset<Row> dataFrame,
                                                     String tablePath) throws DataStorageException {
        logger.info("Appending {} records to deltalake table: {}", dataFrame.count(), tablePath);
        storage.append(tablePath, dataFrame);
        logger.info("Append completed successfully");
        storage.updateDeltaManifestForTable(spark, tablePath);
    }

    private void appendDataAndUpdateManifestForTable(SparkSession spark,
                                                     Dataset<Row> dataFrame,
                                                     String tablePath,
                                                     SourceReference.PrimaryKey primaryKey) throws DataStorageException {
        logger.info("Appending {} records to deltalake table: {}", dataFrame.count(), tablePath);
        // TODO: DPR-309 - use operation to determine how to write to delta table
        storage.appendDistinct(tablePath, dataFrame, primaryKey);
        logger.info("Append completed successfully");
        storage.updateDeltaManifestForTable(spark, tablePath);
    }

    private void applyIncrementalRecordsAndUpdateManifest(
            SparkSession spark,
            String destinationPath,
            String primaryKey,
            Dataset<Row> validRecords) {

        logger.info("Applying {} CDC records to deltalake table: {}", validRecords.count(), destinationPath);

        validRecords.collectAsList().forEach(row -> {
                Optional<DMS_3_4_6.Operation> optionalOperation = getOperation(row.getAs(OPERATION));
                if (optionalOperation.isPresent()) {
                    val list = new ArrayList<Row>();
                    list.add(row);
                    val dataFrame = spark.createDataFrame(list, row.schema());

                    val operation = optionalOperation.get();
                    try {
                        switch (operation) {
                            case Insert:
                                storage.appendDistinct(destinationPath, dataFrame, primaryKey);
                                break;
                            case Update:
                                storage.update(destinationPath, dataFrame, primaryKey);
                                break;
                            case Delete:
                                storage.deleteRecords(destinationPath, dataFrame, primaryKey);
                                break;
                            default:
                                break;
                        }
                    } catch (DataStorageException ex) {
                        logger.warn("Failed to {}: \n{}\nto{}", operation.getName(), row.json(), destinationPath);
                    }
                }
            }
        );

        logger.info("CDC records successfully applied");
        storage.updateDeltaManifestForTable(spark, destinationPath);
    }
}
