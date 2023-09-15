package uk.gov.justice.digital.client.kinesis;

import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.Job$;
import io.micronaut.context.annotation.Bean;
import jakarta.inject.Inject;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisInitialPositions;
import org.apache.spark.streaming.kinesis.KinesisInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassTag$;
import uk.gov.justice.digital.config.JobArguments;

@Bean
public class KinesisReader {

    private static final Logger logger = LoggerFactory.getLogger(KinesisReader.class);

    private final JavaStreamingContext streamingContext;
    private final Job$ job;
    private final JavaDStream<byte[]> kinesisStream;

    @Inject
    public KinesisReader(
            JobArguments jobArguments,
            String jobName,
            SparkContext sparkContext
    ) {
        streamingContext = new JavaStreamingContext(
                JavaSparkContext.fromSparkContext(sparkContext),
                jobArguments.getKinesisReaderBatchDuration()
        );

        GlueContext glueContext = new GlueContext(sparkContext);
        job = Job$.MODULE$.init(jobName, glueContext, jobArguments.getConfig());

        kinesisStream = JavaDStream.fromDStream(
                KinesisInputDStream.builder()
                        .streamingContext(streamingContext)
                        .endpointUrl(jobArguments.getAwsKinesisEndpointUrl())
                        .regionName(jobArguments.getAwsRegion())
                        .streamName(jobArguments.getKinesisReaderStreamName())
                        .initialPosition(new KinesisInitialPositions.Latest())
                        .checkpointAppName(jobName)
                        .build(),
                // We need to pass a Scala classtag which looks a little ugly in Java.
                ClassTag$.MODULE$.apply(byte[].class)
        );

        logger.info("Configuration - endpointUrl: {} awsRegion: {} streamName: {} batchDuration: {}",
                jobArguments.getAwsKinesisEndpointUrl(),
                jobArguments.getAwsRegion(),
                jobArguments.getKinesisReaderStreamName(),
                jobArguments.getKinesisReaderBatchDuration()
        );
    }

    public void setBatchProcessor(VoidFunction<JavaRDD<byte[]>> batchProcessor) {
        kinesisStream.foreachRDD(batchProcessor);
    }

    public void startAndAwaitTermination() throws InterruptedException {
        this.start();
        streamingContext.awaitTermination();
        logger.info("KinesisReader terminated");
        job.commit();
        logger.info("Glue job committed");
    }

    public void start() {
        logger.info("Starting KinesisReader");
        streamingContext.start();
        logger.info("KinesisReader started");
    }

    public void stop() {
        logger.info("Stopping KinesisReader");
        job.commit();
        logger.info("Glue job committed");
    }

}
