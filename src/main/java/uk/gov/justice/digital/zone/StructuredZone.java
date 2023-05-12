package uk.gov.justice.digital.zone;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.job.udf.JsonValidator;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.domain.model.SourceReference;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.Map;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_json;
import static org.apache.spark.sql.functions.from_json;
import static uk.gov.justice.digital.job.model.Columns.*;

@Singleton
public class StructuredZone extends Zone {

    private static final Logger logger = LoggerFactory.getLogger(StructuredZone.class);

    private static final Map<String, String> jsonOptions = Collections.singletonMap("ignoreNullFields", "false");

    private final String structuredPath;
    private final String violationsPath;
    private final DataStorageService storage;

    @Inject
    public StructuredZone(JobParameters jobParameters, DataStorageService storage) {
        this.structuredPath = jobParameters.getStructuredS3Path()
            .orElseThrow(() -> new IllegalStateException(
                "structured s3 path not set - unable to create StructuredZone instance"
            ));
        this.violationsPath = jobParameters.getViolationsS3Path()
            .orElseThrow(() -> new IllegalStateException(
                "violations s3 path not set - unable to create StructuredZone instance"
            ));
        this.storage = storage;
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, Row table) {

        val rowCount = dataFrame.count();

        logger.info("Processing batch with {} records", rowCount);

        val startTime = System.currentTimeMillis();

        String sourceName = table.getAs(SOURCE);
        String tableName = table.getAs(TABLE);

        val sourceReference = SourceReferenceService.getSourceReference(sourceName, tableName);

        logger.info("Processing {} records for {}/{}", rowCount, sourceName, tableName);

        val structuredDataFrame = sourceReference.isPresent()
            ? handleSchemaFound(spark, dataFrame, sourceReference.get())
            : handleNoSchemaFound(spark, dataFrame, sourceName, tableName);

        logger.info("Processed data frame with {} rows in {}ms",
                rowCount,
                System.currentTimeMillis() - startTime
        );

        return structuredDataFrame;
    }

    protected Dataset<Row> handleSchemaFound(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference) {

        val tablePath = this.storage.getTablePath(structuredPath, sourceReference);

        val validationFailedViolationPath = this.storage.getTablePath(
            violationsPath,
            sourceReference
        );

        val validatedDataFrame = validateJsonData(
            dataFrame,
            sourceReference.getSchema(),
            sourceReference.getSource(),
            sourceReference.getTable()
        );

        handleInValidRecords(spark,
            validatedDataFrame,
            sourceReference.getSource(),
            sourceReference.getTable(),
            validationFailedViolationPath
        );

        return handleValidRecords(spark, validatedDataFrame, tablePath);
    }

    protected Dataset<Row> validateJsonData(Dataset<Row> dataFrame, StructType schema, String source, String table) {

        logger.info("Validating data against schema: {}/{}", source, table);

        val jsonValidator = JsonValidator.createAndRegister(schema, dataFrame.sparkSession(), source, table);
        System.out.println(jsonValidator);
        return dataFrame
            .select(col(DATA), col(METADATA))
            .withColumn(PARSED_DATA, from_json(col(DATA), schema, jsonOptions))
            .withColumn(VALID, jsonValidator.apply(col(DATA), to_json(col(PARSED_DATA), jsonOptions)));
    }

    protected Dataset<Row> handleValidRecords(SparkSession spark, Dataset<Row> dataFrame, String destinationPath) {
        val validRecords = dataFrame
            .select(col(PARSED_DATA), col(VALID))
            .filter(col(VALID).equalTo(true))
            .select(PARSED_DATA + ".*");

        val validRecordsCount = validRecords.count();

        if (validRecordsCount > 0) {
            logger.info("Writing {} valid records", validRecordsCount);
            appendDataAndUpdateManifestForTable(spark, validRecords, destinationPath);
            return validRecords;
        }
        else return createEmptyDataFrame(dataFrame);
    }

    protected void handleInValidRecords(SparkSession spark, Dataset<Row> dataFrame, String source,
                                      String table, String destinationPath) {
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

    protected Dataset<Row> handleNoSchemaFound(SparkSession spark, Dataset<Row> dataFrame, String source, String table) {
        logger.error("Structured Zone Violation - No schema found for {}/{} - writing {} records",
            source,
            table,
            dataFrame.count()
        );

        val missingSchemaRecords = dataFrame
            .select(col(DATA), col(METADATA))
            .withColumn(ERROR, lit(String.format("Schema does not exist for %s/%s", source, table)));

        appendDataAndUpdateManifestForTable(spark, missingSchemaRecords,
                this.storage.getTablePath(violationsPath, source, table));
        return createEmptyDataFrame(dataFrame);
    }

    private void appendDataAndUpdateManifestForTable(SparkSession spark, Dataset<Row> dataFrame, String tablePath) {
        logger.info("Appending {} records to deltalake table: {}", dataFrame.count(), tablePath);
        this.storage.append(tablePath, dataFrame);
        logger.info("Append completed successfully");
        this.storage.updateDeltaManifestForTable(spark, tablePath);
    }
}
