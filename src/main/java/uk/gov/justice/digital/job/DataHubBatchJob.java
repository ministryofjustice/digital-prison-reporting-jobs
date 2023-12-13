package uk.gov.justice.digital.job;

import com.amazonaws.services.glue.util.Job;
import com.google.common.annotations.VisibleForTesting;
import io.micronaut.configuration.picocli.PicocliRunner;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.job.batchprocessing.S3BatchProcessor;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.TableDiscoveryService;
import uk.gov.justice.digital.service.ViolationService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static uk.gov.justice.digital.config.JobProperties.SPARK_JOB_NAME_PROPERTY;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_LOAD;

@Singleton
@CommandLine.Command(name = "DataHubBatchJob")
public class DataHubBatchJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataHubBatchJob.class);

    private final JobArguments arguments;
    private final JobProperties properties;
    private final SparkSessionProvider sparkSessionProvider;
    private final TableDiscoveryService tableDiscoveryService;
    private final S3BatchProcessor batchProcessor;
    private final S3DataProvider dataProvider;
    private final SourceReferenceService sourceReferenceService;
    private final ViolationService violationService;

    @Inject
    public DataHubBatchJob(
            JobArguments arguments,
            JobProperties properties,
            SparkSessionProvider sparkSessionProvider,
            TableDiscoveryService tableDiscoveryService,
            S3BatchProcessor batchProcessor,
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
        logger.info("Job started");
        PicocliRunner.run(DataHubBatchJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        val startTime = System.currentTimeMillis();

        logger.info("Running DataHubBatchJob");
        try {
            boolean runLocal = System.getProperty(SPARK_JOB_NAME_PROPERTY) == null;
            if(runLocal) {
                logger.info("Running locally");
                SparkConf sparkConf = new SparkConf().setAppName("DataHubBatchJob local").setMaster("local[*]");
                SparkSession spark = sparkSessionProvider.getConfiguredSparkSession(sparkConf, arguments.getLogLevel());
                runJob(spark);
            } else {
                logger.info("Running in Glue");
                String jobName = properties.getSparkJobName();
                val glueContext = sparkSessionProvider.createGlueContext(jobName, arguments.getLogLevel());
                Job.init(jobName, glueContext, arguments.getConfig());
                SparkSession spark = glueContext.getSparkSession();
                runJob(spark);
                Job.commit();
            }
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
        logger.info("DataHubBatchJob completed in {}ms", System.currentTimeMillis() - startTime);
    }

    /**
     * The main entry point for testing a batch job to process raw data for all tables.
     */
    @VisibleForTesting
    void runJob(SparkSession sparkSession) throws IOException, DataStorageException {
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
                        violationService.handleNoSchemaFoundS3(sparkSession, dataFrame, schema, table, STRUCTURED_LOAD);
                    }
                } catch (Exception e) {
                    // Due to Scala not advertising checked Exceptions we have to catch a broader Exception and
                    // then check its type rather than catching the actual Exception type we want
                    if(e instanceof SparkException && ((SparkException)e).getMessage().startsWith("Failed merging schema")) {
                        String msg = String.format("Violation - Incompatible schemas across multiple files for %s.%s", schema, table);
                        logger.warn(msg, e);
                        violationService.writeBatchDataToViolations(sparkSession, schema, table, msg);
                    } else {
                        throw e;
                    }
                }
            } else {
                logger.warn("No paths found for table {}.{}", schema, table);
            }
        }
        logger.info("Finished processing Raw {} table by table in {}ms", rawPath, System.currentTimeMillis() - startTime);
    }
}
