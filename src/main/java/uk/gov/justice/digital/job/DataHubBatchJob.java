package uk.gov.justice.digital.job;

import com.amazonaws.services.glue.util.Job;
import com.google.common.annotations.VisibleForTesting;
import io.micronaut.configuration.picocli.PicocliRunner;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import scala.collection.JavaConverters;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.job.batchprocessing.S3BatchProcessor;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.TableDiscoveryService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static uk.gov.justice.digital.config.JobProperties.SPARK_JOB_NAME_PROPERTY;

@Singleton
@CommandLine.Command(name = "DataHubBatchJob")
public class DataHubBatchJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataHubBatchJob.class);

    private final JobArguments arguments;
    private final JobProperties properties;
    private final SparkSessionProvider sparkSessionProvider;
    private final TableDiscoveryService tableDiscoveryService;
    private final S3BatchProcessor batchProcessor;

    @Inject
    public DataHubBatchJob(
            JobArguments arguments,
            JobProperties properties,
            SparkSessionProvider sparkSessionProvider,
            TableDiscoveryService tableDiscoveryService,
            S3BatchProcessor batchProcessor
    ) {
        this.arguments = arguments;
        this.properties = properties;
        this.sparkSessionProvider = sparkSessionProvider;
        this.tableDiscoveryService = tableDiscoveryService;
        this.batchProcessor = batchProcessor;
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
     * The main entry point for starting a batch job to process raw data for all tables.
     */
    @VisibleForTesting
    void runJob(SparkSession sparkSession) throws IOException {
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
            val filePaths = JavaConverters.asScalaIteratorConverter(entry.getValue().iterator()).asScala().toSeq();
            if(filePaths.nonEmpty()) {
                val dataFrame = sparkSession.read().parquet(filePaths);
                logger.info("Schema for {}.{}: \n{}", schema, table, dataFrame.schema().treeString());
                batchProcessor.processBatch(sparkSession, schema, table, dataFrame);
                logger.info("Processed table {}.{} in {}ms", schema, table, System.currentTimeMillis() - tableStartTime);
            } else {
                logger.warn("No paths found for table {}.{}", schema, table);
            }
        }
        logger.info("Finished processing Raw {} table by table in {}ms", rawPath, System.currentTimeMillis() - startTime);
    }
}
