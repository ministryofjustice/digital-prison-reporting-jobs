package uk.gov.justice.digital.client.kinesis;

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

        kinesisStream = JavaDStream.fromDStream(
                KinesisInputDStream.builder()
                        .streamingContext(streamingContext)
                        .endpointUrl(jobArguments.getAwsKinesisEndpointUrl())
                        .regionName(jobArguments.getAwsRegion())
                        .streamName(jobArguments.getKinesisReaderStreamName())
                        .initialPosition(new KinesisInitialPositions.TrimHorizon())
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
        try {
            streamingContext.awaitTermination();
        } catch(Throwable e) {
            logger.warn("Throwing out of startAndAwaitTermination", e);
            throw e;
        } finally {
            logger.warn("Streaming context terminating");
        }
        logger.info("KinesisReader terminated");
    }

    public void start() {
        logger.info("Starting KinesisReader");
        streamingContext.start();
        logger.info("KinesisReader started");
    }

    public void stop() {
        logger.info("Stopping KinesisReader");
        streamingContext.stop();
        logger.info("KinesisReader terminated");
    }

}
