package uk.gov.justice.digital.job;

import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.Job;
import com.amazonaws.services.glue.util.JsonOptions;
import io.micronaut.configuration.picocli.PicocliRunner;
import jakarta.inject.Inject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import scala.collection.JavaConverters;
import scala.runtime.BoxedUnit;
import uk.gov.justice.digital.client.kinesis.KinesisDataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.converter.Converter;
import uk.gov.justice.digital.converter.dms.DMS_3_4_7;
import uk.gov.justice.digital.job.batchprocessing.BatchProcessor;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.provider.SparkSessionProvider;

import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

/**
 * Job that reads DMS 3.4.7 load events from a Kinesis stream and processes the data as follows
 * - validates the data to ensure it conforms to the expected input format - DPR-341
 * - writes the raw data to the raw zone in s3
 * - validates the data to ensure it confirms to the appropriate table schema
 * - writes this validated data to the structured zone in s3
 */
@Singleton
@Command(name = "DataHubJob")
public class DataHubJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataHubJob.class);

    private final JobArguments arguments;
    private final JobProperties properties;
    private final SparkSessionProvider sparkSessionProvider;
    private final BatchProcessor batchProcessor;
    private final KinesisDataProvider kinesisDataProvider;


    @Inject
    public DataHubJob(
            JobArguments arguments,
            JobProperties properties,
            SparkSessionProvider sparkSessionProvider,
            KinesisDataProvider kinesisDataProvider,
            BatchProcessor batchProcessor
    ) {
        logger.info("Initializing DataHubJob");
        this.arguments = arguments;
        this.properties = properties;
        this.sparkSessionProvider = sparkSessionProvider;
        this.kinesisDataProvider = kinesisDataProvider;
        this.batchProcessor = batchProcessor;
        logger.info("DataHubJob initialization complete");
    }

    public static void main(String[] args) {
        logger.info("Job started");
        PicocliRunner.run(DataHubJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        String jobName = properties.getSparkJobName();
        GlueContext glueContext = sparkSessionProvider.createGlueContext(jobName, arguments.getLogLevel());
        SparkSession sparkSession = glueContext.getSparkSession();

        logger.info("Initialising Job");
        Job.init(jobName, glueContext, arguments.getConfig());

        logger.info("Initialising data source");
        Dataset<Row> sourceDf = kinesisDataProvider.getSourceData(glueContext, arguments);

        logger.info("Initialising converter");
        Converter<Dataset<Row>, Dataset<Row>> converter = new DMS_3_4_7(sparkSession);

        logger.info("Initialising per batch processing");
        glueContext.forEachBatch(sourceDf, (batch, batchId) -> {
            try {
                batchProcessor.processBatch(sparkSession, converter, batch);
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    logger.error("Kinesis job interrupted", e);
                } else {
                    logger.error("Exception occurred during streaming job", e);
                    System.exit(1);
                }
            }
            // return type is Unit since we must use the Scala API
            return BoxedUnit.UNIT;
        }, createBatchOptions());

        logger.info("Committing Job");
        Job.commit();
    }

    private JsonOptions createBatchOptions() {
        // See https://docs.aws.amazon.com/glue/latest/dg/glue-etl-scala-apis-glue-gluecontext.html#glue-etl-scala-apis-glue-gluecontext-defs-forEachBatch
        Map<String, String> batchProcessingOptions = new HashMap<>();
        batchProcessingOptions.put("windowSize", arguments.getBatchDuration());
        batchProcessingOptions.put("checkpointLocation", arguments.getCheckpointLocation());
        batchProcessingOptions.put("batchMaxRetries", Integer.toString(arguments.getBatchMaxRetries()));
        logger.info("Batch Options: {}", batchProcessingOptions);
        return new JsonOptions(JavaConverters.mapAsScalaMap(batchProcessingOptions));
    }
}
