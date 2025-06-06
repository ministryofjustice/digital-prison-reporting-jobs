package uk.gov.justice.digital.job;

import com.google.common.annotations.VisibleForTesting;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.SparkSession;
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

import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    @Inject
    public DataHubBatchJob(
            JobArguments arguments,
            JobProperties properties,
            SparkSessionProvider sparkSessionProvider,
            TableDiscoveryService tableDiscoveryService,
            BatchProcessor batchProcessor,
            S3DataProvider dataProvider,
            SourceReferenceService sourceReferenceService,
            ViolationService violationService) {
        this.arguments = arguments;
        this.properties = properties;
        this.sparkSessionProvider = sparkSessionProvider;
        this.tableDiscoveryService = tableDiscoveryService;
        this.batchProcessor = batchProcessor;
        this.dataProvider = dataProvider;
        this.sourceReferenceService = sourceReferenceService;
        this.violationService = violationService;
    }

    public static void main(String[] args) {
        PicocliMicronautExecutor.execute(DataHubBatchJob.class, args);
    }

    @Override
    public void run() {
        val startTime = System.currentTimeMillis();
        SparkJobRunner.run("DataHubBatchJob", arguments, properties, sparkSessionProvider, logger, this::runJob);
        logger.info("DataHubBatchJob completed in {}ms", System.currentTimeMillis() - startTime);
    }

    /**
     * The main entry point for testing a batch job to process raw data for all tables.
     */
    @VisibleForTesting
    void runJob(SparkSession sparkSession) throws DataStorageException {
        val startTime = System.currentTimeMillis();
        String rawPath = arguments.getRawS3Path();
        logger.info("Processing Raw {} table by table", rawPath);
        Map<ImmutablePair<String, String>, List<String>> pathsByTable = tableDiscoveryService.discoverBatchFilesToLoad(rawPath, sparkSession);
        if(pathsByTable.isEmpty()) {
            String msg = "No tables found under " + rawPath;
            logger.error(msg);
            throw new RuntimeException(msg);
        }
        for (val entry: pathsByTable.entrySet()) {
            val tableStartTime = System.currentTimeMillis();
            val schema = entry.getKey().getLeft();
            val table = entry.getKey().getRight();
            logger.info("Processing table {}.{}", schema, table);
            val filePaths = entry.getValue();
            if(!filePaths.isEmpty()) {
                processFilePaths(sparkSession, schema, table, filePaths, tableStartTime);
            } else {
                logger.warn("No paths found for table {}.{}", schema, table);
            }
        }
        logger.info("Finished processing Raw {} table by table in {}ms", rawPath, System.currentTimeMillis() - startTime);
    }

    private void processFilePaths(SparkSession sparkSession, String schema, String table, List<String> filePaths, long tableStartTime) throws DataStorageException {
        Optional<SourceReference> maybeSourceReference = sourceReferenceService.getSourceReference(schema, table);
        try {
            val dataFrame = dataProvider.getBatchSourceData(sparkSession, filePaths);

            logger.info("Schema for {}.{}: \n{}", schema, table, dataFrame.schema().treeString());
            if(maybeSourceReference.isPresent()) {
                SourceReference sourceReference = maybeSourceReference.get();
                batchProcessor.processBatch(sparkSession, sourceReference, dataFrame);
                logger.info("Processed table {}.{} in {}ms", schema, table, System.currentTimeMillis() - tableStartTime);
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
