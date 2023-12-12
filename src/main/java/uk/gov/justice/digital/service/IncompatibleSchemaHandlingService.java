package uk.gov.justice.digital.service;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecutionException;
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;

import java.net.URI;
import java.util.List;

import static java.lang.String.format;
import static uk.gov.justice.digital.common.CommonDataFields.ERROR;
import static uk.gov.justice.digital.common.ResourcePath.tablePath;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_CDC;

public class IncompatibleSchemaHandlingService {

    private static final Logger logger = LoggerFactory.getLogger(IncompatibleSchemaHandlingService.class);

    private final S3DataProvider dataProvider;
    private final TableDiscoveryService tableDiscoveryService;
    private final ViolationService violationService;
    private final JobArguments arguments;
    private final String source;
    private final String table;

    public IncompatibleSchemaHandlingService(
            S3DataProvider dataProvider,
            TableDiscoveryService tableDiscoveryService,
            ViolationService violationService,
            JobArguments arguments,
            String source,
            String table
    ) {
        this.dataProvider = dataProvider;
        this.tableDiscoveryService = tableDiscoveryService;
        this.violationService = violationService;
        this.arguments = arguments;
        this.source = source;
        this.table = table;
    }

    public VoidFunction2<Dataset<Row>, Long> decorate(VoidFunction2<Dataset<Row>, Long> originalFunc) {
        return (df, batchId) -> {
            try {
                originalFunc.call(df, batchId);
            } catch (SparkException e) {
                // We only want to handle a very specific Exception (wrapped in two others) here
                if (e.getCause() instanceof QueryExecutionException &&
                        e.getCause().getCause() instanceof SchemaColumnConvertNotSupportedException) {
                    SchemaColumnConvertNotSupportedException cause =
                            (SchemaColumnConvertNotSupportedException) e.getCause().getCause();
                    String msg = format("Violation - incompatible types for column %s. Tried to use %s but found %s",
                            cause.getColumn(), cause.getLogicalType(), cause.getPhysicalType());
                    logger.warn(msg, e);
                    moveCdcDataToViolations(df.sparkSession(), source, table, msg);
                } else {
                    throw e;
                }
            }
        };
    }

    @VisibleForTesting
    void moveCdcDataToViolations(SparkSession spark, String source, String table, String errorMessage) {
        try {
            String rawRoot = arguments.getRawS3Path();
            String cdcGlobPattern = arguments.getCdcFileGlobPattern();
            String tablePath = tablePath(rawRoot, source, table);
            FileSystem fileSystem = FileSystem.get(URI.create(rawRoot), spark.sparkContext().hadoopConfiguration());
            // We only read data that matches the CDC file glob pattern
            List<String> filePaths = tableDiscoveryService.listFiles(fileSystem, tablePath, cdcGlobPattern);
            logger.info("Moving {} CDC files to violations to avoid schema mismatch", filePaths.size());
            for (String filePath: filePaths) {
                logger.info("Moving {} to violations started", filePath);
                // We need to read the data file-by-file, rather than in a single read call for all files, in case
                // there are multiple files with incompatible schemas which cannot be read and merged in a single read.
                Dataset<Row> df = dataProvider.getBatchSourceData(spark, filePath);
                Dataset<Row> violations = df.withColumn(ERROR, functions.lit(errorMessage));
                violationService.handleViolation(spark, violations, source, table, STRUCTURED_CDC);
                logger.info("Moving {} to violations completed", filePath);
            }
            logger.info("Finished moving all available CDC data to violations to avoid schema mismatch");
        } catch (Exception e) {
            logger.error("Caught unexpected Exception when moving CDC data to violations", e);
            throw new RuntimeException(e);
        }
    }
}
