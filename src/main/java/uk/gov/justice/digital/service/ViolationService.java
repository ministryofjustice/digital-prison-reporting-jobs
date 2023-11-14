package uk.gov.justice.digital.service;

import jakarta.inject.Inject;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;

import javax.inject.Singleton;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static uk.gov.justice.digital.common.CommonDataFields.ERROR;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.DATA;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.METADATA;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.TIMESTAMP;

@Singleton
public class ViolationService {

    private static final Logger logger = LoggerFactory.getLogger(ViolationService.class);

    private final String violationsPath;
    private final DataStorageService storageService;

    /**
     * Allows us to record where a violation occurred.
     */
    public enum ZoneName {
        RAW("Raw"),
        STRUCTURED_LOAD("Structured Load"),
        STRUCTURED_CDC("Structured CDC"),
        CURATED_LOAD("Curated Load"),
        CURATED_CDC("Curated CDC"),
        CDC("CDC");

        private final String name;

        ZoneName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    @Inject
    public ViolationService(
            JobArguments arguments,
            DataStorageService storageService
    ) {
        this.violationsPath = arguments.getViolationsS3Path();
        this.storageService = storageService;
    }

    public void handleRetriesExhausted(
            SparkSession spark,
            Dataset<Row> dataFrame,
            String source,
            String table,
            DataStorageRetriesExhaustedException cause,
            ZoneName zoneName
    ) {
        String violationMessage = format("Violation - Data storage service retries exceeded for %s/%s for %s", source, table, zoneName);
        logger.warn(violationMessage, cause);
        val destinationPath = createValidatedPath(violationsPath, source, table);
        val violationDf = dataFrame
                .select(col(DATA), col(METADATA))
                .withColumn(ERROR, lit(violationMessage));
        try {
            storageService.append(destinationPath, violationDf);
            storageService.updateDeltaManifestForTable(spark, destinationPath);
        } catch (DataStorageException e) {
            String msg = "Could not write violation data";
            logger.error(msg, e);
            // This is a serious problem because we could lose data if we don't stop here
            throw new RuntimeException(msg, e);
        }
    }

    public void handleNoSchemaFound(
            SparkSession spark,
            Dataset<Row> dataFrame,
            String source,
            String table
    ) throws DataStorageException {
        logger.warn("Violation - No schema found for {}/{}", source, table);
        val destinationPath = createValidatedPath(violationsPath, source, table);

        val missingSchemaRecords = dataFrame
                .select(col(DATA), col(METADATA))
                .withColumn(ERROR, lit(format("Schema does not exist for %s/%s", source, table)))
                .drop(OPERATION);

        storageService.append(destinationPath, missingSchemaRecords);
        storageService.updateDeltaManifestForTable(spark, destinationPath);
    }

    public void handleInvalidSchema(
            SparkSession spark,
            Dataset<Row> dataFrame,
            String source,
            String table
    ) throws DataStorageException {
        val errorPrefix = String.format("Record does not match schema %s/%s: ", source, table);
        val validationFailedViolationPath = createValidatedPath(violationsPath, source, table);
        // Write invalid records where schema validation failed
        val invalidRecords = dataFrame.withColumn(ERROR, lit(errorPrefix));

        logger.warn("Violation - Records failed schema validation for source {}, table {}", source, table);
        logger.info("Appending {} records to deltalake table: {}", invalidRecords.count(), validationFailedViolationPath);
        storageService.append(validationFailedViolationPath, invalidRecords.drop(OPERATION, TIMESTAMP));

        logger.info("Append completed successfully");
        storageService.updateDeltaManifestForTable(spark, validationFailedViolationPath);
    }


}
