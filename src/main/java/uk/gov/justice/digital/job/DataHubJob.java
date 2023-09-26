package uk.gov.justice.digital.job;

import com.amazonaws.services.glue.DataSource;
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
import picocli.CommandLine;
import scala.collection.JavaConverters;
import scala.runtime.BoxedUnit;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.converter.dms.DMS_3_4_6;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.provider.SparkSessionProvider;

import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.RECORD_SCHEMA;

@Singleton
@CommandLine.Command(name = "DataHubJob")
public class DataHubJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataHubJob.class);

    private final JobArguments arguments;
    private final JobProperties properties;
    private final BatchProcessorProvider batchProcessorProvider;

    @Inject
    public DataHubJob(
            JobArguments arguments,
            JobProperties properties,
            BatchProcessorProvider batchProcessorProvider
    ) {
        this.arguments = arguments;
        this.properties = properties;
        this.batchProcessorProvider = batchProcessorProvider;
    }

    public static void main(String[] args) {
        logger.info("Job started");
        PicocliRunner.run(DataHubJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        String jobName = properties.getSparkJobName();
        GlueContext glueContext = SparkSessionProvider.createGlueContext(jobName, arguments.getLogLevel());
        SparkSession sparkSession = glueContext.getSparkSession();

        logger.info("Initialising Job");
        Job.init(jobName, glueContext, arguments.getConfig());

        DataSource kinesisDataSource = getKinesisSource(glueContext, arguments);
        Dataset<Row> sourceDf = kinesisDataSource.getDataFrame();
        logger.info("Initialising Kinesis data source");

        BatchProcessor batchProcessor = batchProcessorProvider.createBatchProcessor(sparkSession, new DMS_3_4_6(sparkSession));

        logger.info("Initialising per batch processing");
        glueContext.forEachBatch(sourceDf, (batch, batchId) -> {
            try {
                batchProcessor.processBatch(batch);
            } catch (BatchProcessingRuntimeException e) {
                logger.error("Exiting due to batch processing exception", e);
                // We need to explicitly System.exit when there is an error, rather than throw a RuntimeException, for Glue to set job status correctly
                System.exit(1);
            }
            return BoxedUnit.UNIT;
        }, createBatchOptions());

        logger.info("Committing Job");
        Job.commit();
    }

    private JsonOptions createBatchOptions() {
        Map<String, String> batchProcessingOptions = new HashMap<>();
        batchProcessingOptions.put("windowSize", arguments.getKinesisReaderBatchDuration());
        batchProcessingOptions.put("checkpointLocation", arguments.getCheckpointLocation());
        batchProcessingOptions.put("batchMaxRetries", Integer.toString(arguments.getBatchMaxRetries()));
        logger.info("Batch Options: {}", batchProcessingOptions);
        return new JsonOptions(JavaConverters.mapAsScalaMap(batchProcessingOptions));
    }

    private static DataSource getKinesisSource(GlueContext glueContext, JobArguments arguments) {
        // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-connect-kinesis-home.html
        Map<String, String> kinesisConnectionOptions = new HashMap<>();
        kinesisConnectionOptions.put("streamARN", arguments.getKinesisStreamArn());
        kinesisConnectionOptions.put("startingPosition", arguments.getKinesisStartingPosition());
        // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-json-home.html
        kinesisConnectionOptions.put("classification", "json");
        kinesisConnectionOptions.put("inferSchema", "false");
        kinesisConnectionOptions.put("schema", RECORD_SCHEMA.toDDL());
        logger.info("Kinesis Connection Options: {}", kinesisConnectionOptions);
        JsonOptions connectionOptions = new JsonOptions(JavaConverters.mapAsScalaMap(kinesisConnectionOptions));
        return glueContext.getSource("kinesis", connectionOptions, "", "");
    }
}
