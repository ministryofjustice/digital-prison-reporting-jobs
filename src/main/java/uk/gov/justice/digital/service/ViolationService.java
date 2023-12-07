package uk.gov.justice.digital.service;

import jakarta.inject.Inject;
import lombok.Getter;
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
import static uk.gov.justice.digital.common.ResourcePath.ensureEndsWithSlash;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.DATA;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.METADATA;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;

@Singleton
public class ViolationService {

    private static final Logger logger = LoggerFactory.getLogger(ViolationService.class);

    private final String violationsPath;
    private final DataStorageService storageService;

    /**
     * Allows us to record where a violation occurred.
     */
    public enum ZoneName {
        RAW("Raw", "raw"),
        STRUCTURED_LOAD("Structured Load", "structured"),
        STRUCTURED_CDC("Structured CDC", "structured"),
        CURATED_LOAD("Curated Load", "curated"),
        CURATED_CDC("Curated CDC", "curated");

        private final String name;
        @Getter
        private final String path;

        ZoneName(String name, String path) {
            this.name = name;
            this.path = path;
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

    public void handleRetriesExhaustedS3(
            SparkSession spark,
            Dataset<Row> dataFrame,
            String source,
            String table,
            DataStorageRetriesExhaustedException cause,
            ZoneName zoneName
    ) {
        String violationMessage = format("Violation - Data storage service retries exceeded for %s/%s for %s", source, table, zoneName);
        logger.warn(violationMessage, cause);
        val invalidRecords = dataFrame.withColumn(ERROR, lit(violationMessage));
        try {
            handleViolation(spark, invalidRecords, source, table, zoneName);
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

    public void handleNoSchemaFoundS3(
            SparkSession spark,
            Dataset<Row> dataFrame,
            String source,
            String table,
            ZoneName zoneName
    ) throws DataStorageException {
        logger.warn("Violation - No schema found for {}/{}", source, table);
        val invalidRecords = dataFrame
                .withColumn(ERROR, lit(format("Schema does not exist for %s/%s", source, table)));

        handleViolation(spark, invalidRecords, source, table, zoneName);
    }

    /**
     * Handle violations with error column already present on the DataFrame.
     */
    public void handleViolation(
            SparkSession spark,
            Dataset<Row> invalidRecords,
            String source,
            String table,
            ZoneName zoneName
    ) throws DataStorageException {
        val destinationPath = fullTablePath(source, table, zoneName);
        logger.warn("Violation - for source {}, table {}", source, table);
        logger.info("Appending {} records to deltalake table: {}", invalidRecords.count(), destinationPath);
        storageService.append(destinationPath, invalidRecords);

        logger.info("Append completed successfully");
        storageService.updateDeltaManifestForTable(spark, destinationPath);
    }

    private String fullTablePath(String source, String table, ZoneName zone) {
        String root = ensureEndsWithSlash(violationsPath) + zone.getPath() + "/";
        return createValidatedPath(root, source, table);
    }


}
