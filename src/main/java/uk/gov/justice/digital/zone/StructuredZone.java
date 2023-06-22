package uk.gov.justice.digital.zone;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.job.udf.JsonValidator;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;

import javax.inject.Inject;

import java.util.Collections;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.to_json;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.*;

public abstract class StructuredZone extends Zone implements DeltaWriter {

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
    public StructuredZone(
            JobArguments jobArguments,
            DataStorageService storage,
            SourceReferenceService sourceReferenceService
    ) {
        this.storage = storage;
        this.structuredPath = jobArguments.getStructuredS3Path();
        this.violationsPath = jobArguments.getViolationsS3Path();
        this.sourceReferenceService = sourceReferenceService;
    }

    public Dataset<Row> process(SparkSession spark, Dataset<Row> filteredRecords, Row table) throws DataStorageException {

        val sortedRecords = filteredRecords.orderBy(col(TIMESTAMP));
        val rowCount = sortedRecords.count();

        logger.info("Processing batch with {} records", rowCount);

        val startTime = System.currentTimeMillis();

        String sourceName = table.getAs(SOURCE);
        String tableName = table.getAs(TABLE);

        val sourceReference = sourceReferenceService.getSourceReference(sourceName, tableName);

        logger.info("Processing {} records for {}/{}", rowCount, sourceName, tableName);

        val structuredDataFrame = sourceReference.isPresent()
                ? handleSchemaFound(spark, sortedRecords, sourceReference.get())
                : handleNoSchemaFound(spark, sortedRecords, sourceName, tableName);

        logger.info("Processed batch with {} rows in {}ms", rowCount, System.currentTimeMillis() - startTime);

        return structuredDataFrame;
    }

    private Dataset<Row> handleSchemaFound(
            SparkSession spark,
            Dataset<Row> dataFrame,
            SourceReference sourceReference
    ) throws DataStorageException {
        val source = sourceReference.getSource();
        val table = sourceReference.getTable();

        val tablePath = createValidatedPath(structuredPath, source, table);
        val validationFailedViolationPath = createValidatedPath(violationsPath, source, table);

        val validatedDataFrame = validateJsonData(spark, dataFrame, sourceReference.getSchema(), source, table);

        handleInvalidRecords(spark, validatedDataFrame, source, table, validationFailedViolationPath);

        return handleValidRecords(spark, validatedDataFrame, tablePath, sourceReference.getPrimaryKey());
    }

    private Dataset<Row> handleNoSchemaFound(
            SparkSession spark,
            Dataset<Row> dataFrame,
            String source,
            String table
    ) throws DataStorageException {
        logger.error("Structured Zone Violation - No schema found for {}/{} - writing {} records",
                source,
                table,
                dataFrame.count()
        );

        val missingSchemaRecords = dataFrame
                .select(col(DATA), col(METADATA))
                .withColumn(ERROR, lit(String.format("Schema does not exist for %s/%s", source, table)));

        writeInvalidRecords(spark, storage, createValidatedPath(violationsPath, source, table), missingSchemaRecords);

        return createEmptyDataFrame(dataFrame);
    }

    private void handleInvalidRecords(
            SparkSession spark,
            Dataset<Row> dataFrame,
            String source,
            String table,
            String destinationPath
    ) throws DataStorageException {
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
            writeInvalidRecords(spark, storage, destinationPath, invalidRecords);
        }
    }

    private Dataset<Row> handleValidRecords(
            SparkSession spark,
            Dataset<Row> dataFrame,
            String destinationPath,
            SourceReference.PrimaryKey primaryKey
    ) throws DataStorageException {
        val validRecords = dataFrame.filter(col(VALID).equalTo(true)).select(PARSED_DATA + ".*", OPERATION);
        val validRecordsCount = validRecords.count();

        if (validRecordsCount > 0) {
            logger.info("Writing {} valid records", validRecordsCount);
            writeValidRecords(spark, storage, destinationPath, primaryKey, validRecords);

            return validRecords;
        } else {
            logger.info("No valid records found");
            return createEmptyDataFrame(dataFrame);
        }
    }

    private Dataset<Row> validateJsonData(
            SparkSession spark,
            Dataset<Row> dataFrame,
            StructType schema,
            String source,
            String table
    ) {
        logger.info("Validating data against schema: {}/{}", source, table);
        val jsonValidator = JsonValidator.createAndRegister(schema, spark, source, table);

        return dataFrame
                .select(col(DATA), col(METADATA), col(OPERATION))
                .withColumn(PARSED_DATA, from_json(col(DATA), schema, jsonOptions))
                .withColumn(VALID, jsonValidator.apply(col(DATA), to_json(col(PARSED_DATA), jsonOptions)));
    }

    @Override
    public void writeInvalidRecords(
            SparkSession spark,
            DataStorageService storage,
            String tablePath,
            Dataset<Row> invalidRecords
    ) throws DataStorageException {
        logger.info("Appending {} records to deltalake table: {}", invalidRecords.count(), tablePath);
        storage.append(tablePath, invalidRecords.drop(OPERATION));
        logger.info("Append completed successfully");
        storage.updateDeltaManifestForTable(spark, tablePath);
    }

}
