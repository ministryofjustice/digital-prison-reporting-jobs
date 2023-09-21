package uk.gov.justice.digital.job;

import com.amazonaws.services.glue.DataSource;
import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.GlueArgParser;
import com.amazonaws.services.glue.util.Job;
import com.amazonaws.services.glue.util.JsonOptions;
import io.micronaut.configuration.picocli.PicocliRunner;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import scala.collection.JavaConverters;
import scala.runtime.BoxedUnit;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.job.context.MicronautContext;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.RECORD_SCHEMA;

@Singleton
@CommandLine.Command(name = "MoreAdvancedGlueStream")
public class MoreAdvancedGlueStream implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MoreAdvancedGlueStream.class);

    private static volatile String[] argArray;
    private final JobArguments arguments;
//    private final JobProperties properties;
    private final BatchProcessorProvider batchProcessorProvider;

    @Inject
    public MoreAdvancedGlueStream(
            JobArguments arguments,
//            JobProperties properties,
            BatchProcessorProvider batchProcessorProvider
    ) {
        this.arguments = arguments;
//        this.properties = properties;
        this.batchProcessorProvider = batchProcessorProvider;
    }

    public static void main(String[] args) {
        logger.info("Job started");
        argArray = args;
        PicocliRunner.run(MoreAdvancedGlueStream.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        SparkContext spark = new SparkContext();
        spark.setLogLevel("INFO");
        GlueContext glueContext = new GlueContext(spark);
//        SparkSession sparkSession = glueContext.getSparkSession();
//        Job.init(properties.getSparkJobName(), glueContext, arguments.getConfig());
        scala.collection.immutable.Map<String, String> parsedArgs = GlueArgParser.getResolvedOptions(argArray, new String[]{"JOB_NAME"});
        Job.init(parsedArgs.apply("JOB_NAME"), glueContext, arguments.getConfig());
//        Job.init(parsedArgs.apply("JOB_NAME"), glueContext, JavaConverters.<String, String>mapAsJavaMap(parsedArgs));

        DataSource kinesisDataSource = glueGetSource(glueContext);
        Dataset<Row> sourceDf = kinesisDataSource.getDataFrame();

        Map<String, String> batchProcessingOptions = new HashMap<>();
        batchProcessingOptions.put("windowSize", "30 seconds");
        batchProcessingOptions.put("checkpointLocation", "s3://dpr-working-development/checkpoints");
        batchProcessingOptions.put("batchMaxRetries", "3");
        JsonOptions batchOptions = new JsonOptions(JavaConverters.mapAsScalaMap(batchProcessingOptions));

//        BatchProcessor batchProcessor = batchProcessorProvider.createBatchProcessor(sparkSession);

        glueContext.forEachBatch(sourceDf, (batch, batchId) -> {
            long cnt = batch.count();
            logger.info("Batch saw {} records", cnt);
//            batchProcessor.processBatch(batch);
            return BoxedUnit.UNIT;
        }, batchOptions);


        Job.commit();
    }

    private static DataSource glueGetSource(GlueContext glueContext) {
        // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-connect-kinesis-home.html
        Map<String, String> kinesisConnectionOptions = new HashMap<>();
        kinesisConnectionOptions.put("streamARN", "arn:aws:kinesis:eu-west-2:771283872747:stream/dpr-kinesis-ingestor-development");
        kinesisConnectionOptions.put("startingPosition", "TRIM_HORIZON");
        // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-json-home.html
        kinesisConnectionOptions.put("classification", "json");
        kinesisConnectionOptions.put("inferSchema", "false");
        kinesisConnectionOptions.put("schema", RECORD_SCHEMA.toDDL());
        JsonOptions connectionOptions = new JsonOptions(JavaConverters.mapAsScalaMap(kinesisConnectionOptions));
        return glueContext.getSource("kinesis", connectionOptions, "", "");
    }
}
