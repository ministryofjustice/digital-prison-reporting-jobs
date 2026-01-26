package uk.gov.justice.digital.service;

import jakarta.inject.Inject;
import lombok.Getter;
import lombok.val;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.metrics.MetricReportingService;

import javax.inject.Singleton;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;
import static uk.gov.justice.digital.common.CommonDataFields.ERROR;
import static uk.gov.justice.digital.common.CommonDataFields.ERROR_RAW;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.common.ResourcePath.ensureEndsWithSlash;
import static uk.gov.justice.digital.common.ResourcePath.tablePath;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_CDC;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_LOAD;

@Singleton
public class ViolationService {

    private static final Logger logger = LoggerFactory.getLogger(ViolationService.class);

    private final JobArguments arguments;
    private final DataStorageService storageService;
    private final S3DataProvider dataProvider;
    private final TableDiscoveryService tableDiscoveryService;
    private final MetricReportingService metricReportingService;

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
            DataStorageService storageService,
            S3DataProvider dataProvider,
            TableDiscoveryService tableDiscoveryService,
            MetricReportingService metricReportingService
    ) {
        this.arguments = arguments;
        this.storageService = storageService;
        this.dataProvider = dataProvider;
        this.tableDiscoveryService = tableDiscoveryService;
        this.metricReportingService = metricReportingService;
    }

    public void handleRetriesExhausted(
            SparkSession spark,
            Dataset<Row> dataFrame,
            String source,
            String table,
            DataStorageRetriesExhaustedException cause,
            ZoneName zoneName
    ) throws DataStorageException {
        String violationMessage = format("Violation - Data storage service retries exceeded for %s/%s for %s", source, table, zoneName);
        logger.warn(violationMessage, cause);
        val invalidRecords = dataFrame.withColumn(ERROR, lit(violationMessage));
        handleViolation(spark, invalidRecords, source, table, zoneName);
    }

    public void handleNoSchemaFound(
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
     * Writes all CDC data in the table's input directory to violations.
     */
    public void writeCdcDataToViolations(SparkSession spark, String source, String table, String errorMessage) throws DataStorageException {
        writeDataToViolations(spark, source, table, errorMessage, arguments.getCdcFileGlobPattern(), STRUCTURED_CDC);
    }
    /**
     * Writes all Batch data in the table's input directory to violations.
     */
    public void writeBatchDataToViolations(SparkSession spark, String source, String table, String errorMessage) throws DataStorageException {
        writeDataToViolations(spark, source, table, errorMessage, arguments.getBatchLoadFileGlobPattern(), STRUCTURED_LOAD);
    }
    private void writeDataToViolations(
            SparkSession spark,
            String source,
            String table,
            String errorMessage,
            String fileGlobPattern,
            ZoneName zone
    ) throws DataStorageException {

        try {
            String rawRoot = arguments.getRawS3Path();
            FileSystem fileSystem = FileSystem.get(URI.create(rawRoot), spark.sparkContext().hadoopConfiguration());
            String tablePath = tablePath(rawRoot, source, table);
            // We only read data that matches the file glob pattern
            List<String> filePaths = tableDiscoveryService.listFiles(fileSystem, tablePath, fileGlobPattern);
            logger.info("Moving {} files to violations to avoid schema mismatch", filePaths.size());
            for (String filePath: filePaths) {
                logger.info("Moving {} to violations started", filePath);
                // We need to read the data file-by-file, rather than in a single read call for all files, in case
                // there are multiple files with incompatible schemas which cannot be read and merged in a single read.
                Dataset<Row> df = dataProvider.getBatchSourceData(spark, filePath);
                Dataset<Row> violations = df.withColumn(ERROR, functions.lit(errorMessage));
                handleViolation(spark, violations, source, table, zone);
                logger.info("Moving {} to violations completed", filePath);
            }
            logger.info("Finished moving all available CDC data to violations to avoid schema mismatch");
        } catch (IOException e) {
            throw new DataStorageException("Unexpected Exception when moving CDC data to violations", e);
        }
    }

    /**
     * Handle violations.
     * All violations must ultimately be written through this method, even if indirectly, so that
     * violation metrics are reported correctly.
     * The error column detailing the violation must already be present on the DataFrame.
     */
    public void handleViolation(
            SparkSession spark,
            Dataset<Row> invalidRecords,
            String source,
            String table,
            ZoneName zoneName
    ) throws DataStorageRetriesExhaustedException {
        val destinationPath = fullTablePath(source, table, zoneName);
        logger.warn("Violation - for source {}, table {}, zone: {}", source, table, zoneName);
        logger.info("Appending records to deltalake table: {}", destinationPath);
        Column[] columns = Arrays
                .stream(invalidRecords.columns())
                .filter(c -> !ERROR.equals(c))
                .map(functions::col)
                .toArray(Column[]::new);

        Dataset<Row> toWrite = invalidRecords.select(
                col(ERROR),
                to_json(struct(columns)).as(ERROR_RAW)
        );
        storageService.append(destinationPath, toWrite);

        logger.info("Append completed successfully");
        storageService.updateDeltaManifestForTable(spark, destinationPath);
        long violationsCount = toWrite.count();
        metricReportingService.reportViolationCount(violationsCount);
    }

    private String fullTablePath(String source, String table, ZoneName zone) {
        String root = ensureEndsWithSlash(arguments.getViolationsS3Path()) + zone.getPath() + "/";
        return createValidatedPath(root, source, table);
    }


}
