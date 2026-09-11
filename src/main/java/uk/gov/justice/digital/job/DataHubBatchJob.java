package uk.gov.justice.digital.job;

import com.google.common.annotations.VisibleForTesting;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.DataProviderFailedMergingSchemasException;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.job.batchprocessing.BatchProcessor;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.TableDiscoveryService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.service.metrics.MetricReportingService;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static uk.gov.justice.digital.common.CommonDataFields.withCheckpointField;
import static uk.gov.justice.digital.common.CommonDataFields.withMetadataFields;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_LOAD;

@Singleton
@CommandLine.Command(name = "DataHubBatchJob")
public class DataHubBatchJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataHubBatchJob.class);

    private final JobArguments arguments;
    private final JobProperties properties;
    private final SparkSessionProvider sparkSessionProvider;
    private final TableDiscoveryService tableDiscoveryService;
    private final BatchProcessor batchProcessor;
    private final S3DataProvider dataProvider;
    private final SourceReferenceService sourceReferenceService;
    private final ViolationService violationService;
    private final MetricReportingService metricReportingService;
    private final Clock clock;

    @Inject
    public DataHubBatchJob(
            JobArguments arguments,
            JobProperties properties,
            SparkSessionProvider sparkSessionProvider,
            TableDiscoveryService tableDiscoveryService,
            BatchProcessor batchProcessor,
            S3DataProvider dataProvider,
            SourceReferenceService sourceReferenceService,
            ViolationService violationService,
            MetricReportingService metricReportingService,
            Clock clock) {
        this.arguments = arguments;
        this.properties = properties;
        this.sparkSessionProvider = sparkSessionProvider;
        this.tableDiscoveryService = tableDiscoveryService;
        this.batchProcessor = batchProcessor;
        this.dataProvider = dataProvider;
        this.sourceReferenceService = sourceReferenceService;
        this.violationService = violationService;
        this.metricReportingService = metricReportingService;
        this.clock = clock;
    }

    public static void main(String[] args) {
        PicocliMicronautExecutor.execute(DataHubBatchJob.class, args);
    }

    @Override
    public void run() {
        SparkJobRunner.run("DataHubBatchJob", arguments, properties, sparkSessionProvider, logger, this::runJob);
    }

    /**
     * The main entry point for testing a batch job to process raw data for all tables.
     */
    @VisibleForTesting
    void runJob(SparkSession sparkSession) throws DataStorageException {
        val startTime = clock.millis();
        String rawPath = arguments.getRawS3Path();
        logger.info("Processing Raw {} table by table", rawPath);
        Map<ImmutablePair<String, String>, List<String>> pathsByTable = tableDiscoveryService.discoverBatchFilesToLoad(rawPath, sparkSession);
        if(pathsByTable.isEmpty()) {
            String msg = "No tables found under " + rawPath;
            logger.error(msg);
            throw new RuntimeException(msg);
        }
        for (val entry: pathsByTable.entrySet()) {
            String schema = entry.getKey().getLeft();
            String table = entry.getKey().getRight();
            List<String> filePaths = entry.getValue();
            processTable(sparkSession, schema, table, filePaths);
        }

        // Some configured tables have no raw files at all, so they're missing from pathsByTable entirely.
        // Process those too, passing no file paths, so their curated/structured tables still get created.
        for (val configuredTable: tableDiscoveryService.discoverTablesToProcess()) {
            boolean alreadyProcessedAbove = pathsByTable.containsKey(configuredTable);
            if (!alreadyProcessedAbove) {
                String schema = configuredTable.getLeft();
                String table = configuredTable.getRight();
                processTable(sparkSession, schema, table, Collections.emptyList());
            }
        }

        long timeTakenMillis = clock.millis() - startTime;
        metricReportingService.reportBatchJobTimeTaken(timeTakenMillis);
        logger.info("Finished processing Raw {} table by table in {}ms", rawPath, timeTakenMillis);
    }

    /**
     * Processes a single configured table for this run: from its raw files if it has any, or - so its
     * curated/structured Delta tables still get created with the correct schema - as an empty table if not.
     */
    private void processTable(SparkSession sparkSession, String schema, String table, List<String> filePaths) throws DataStorageException {
        val tableStartTime = clock.millis();
        logger.info("Processing table {}.{}", schema, table);
        if (!filePaths.isEmpty()) {
            processFilePaths(sparkSession, schema, table, filePaths, tableStartTime);
        } else {
            processEmptyTable(sparkSession, schema, table, tableStartTime);
        }
    }

    /**
     * Runs an empty, but correctly schema'd, DataFrame through the batch processor so the structured/curated
     * Delta tables are still created (with no rows) for a table that has no raw batch files this run.
     */
    private void processEmptyTable(SparkSession sparkSession, String schema, String table, long tableStartTime) {
        Optional<SourceReference> maybeSourceReference = sourceReferenceService.getSourceReference(schema, table);
        if (maybeSourceReference.isPresent()) {
            SourceReference sourceReference = maybeSourceReference.get();
            logger.info("No files found for table {}.{} - creating empty table(s) with schema", schema, table);
            Dataset<Row> emptyDataFrame = createEmptyRawDataFrame(sparkSession, sourceReference);
            batchProcessor.processBatch(sparkSession, sourceReference, emptyDataFrame);
            logger.info("Processed table {}.{} in {}ms", schema, table, clock.millis() - tableStartTime);
        } else {
            logger.warn("No source reference for table {}.{} and no files found - skipping", schema, table);
        }
    }

    /**
     * Builds an empty DataFrame shaped like a real raw batch for this table, including the DMS metadata columns,
     * so a table created from it has the same schema as one created from real data.
     */
    private Dataset<Row> createEmptyRawDataFrame(SparkSession sparkSession, SourceReference sourceReference) {
        StructType rawSchema = withCheckpointField(withMetadataFields(sourceReference.getSchema()));
        return sparkSession.createDataFrame(Collections.<Row>emptyList(), rawSchema);
    }

    private void processFilePaths(SparkSession sparkSession, String schema, String table, List<String> filePaths, long tableStartTime) throws DataStorageException {
        Optional<SourceReference> maybeSourceReference = sourceReferenceService.getSourceReference(schema, table);
        try {
            val dataFrame = dataProvider.getBatchSourceData(sparkSession, filePaths);

            logger.info("Schema for {}.{}: \n{}", schema, table, dataFrame.schema().treeString());
            if(maybeSourceReference.isPresent()) {
                SourceReference sourceReference = maybeSourceReference.get();
                batchProcessor.processBatch(sparkSession, sourceReference, dataFrame);
                logger.info("Processed table {}.{} in {}ms", schema, table, clock.millis() - tableStartTime);
            } else {
                logger.warn("No source reference for table {}.{} - writing all data to violations", schema, table);
                violationService.handleNoSchemaFound(sparkSession, dataFrame, schema, table, STRUCTURED_LOAD);
            }
        } catch (DataProviderFailedMergingSchemasException e) {
            String msg = String.format("Violation - Incompatible schemas across multiple files for %s.%s", schema, table);
            logger.warn(msg, e);
            violationService.writeBatchDataToViolations(sparkSession, schema, table, msg);
        }
    }
}
