package uk.gov.justice.digital.client.kinesis;

import lombok.val;
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

public class KinesisReader {

    private static final Logger logger = LoggerFactory.getLogger(KinesisReader.class);

    private final JavaStreamingContext streamingContext;

    public KinesisReader(
            JobArguments jobArguments,
            String jobName,
            SparkContext sparkContext,
            VoidFunction<JavaRDD<byte[]>> batchProcessor
    ) {
        if (jobArguments.isCheckpointEnabled()) {
            logger.info("Checkpointing is enabled. checkpointLocation: {}", jobArguments.getCheckpointLocation());
            streamingContext = JavaStreamingContext.getOrCreate(
                    jobArguments.getCheckpointLocation(),
                    () -> create(jobArguments, jobName, sparkContext, batchProcessor)
            );
        } else {
            logger.info("Checkpointing is disabled.");
            streamingContext = create(jobArguments, jobName, sparkContext, batchProcessor);
        }
    }

    private static JavaStreamingContext create(JobArguments jobArguments,
                        String jobName,
                        SparkContext sparkContext,
                        VoidFunction<JavaRDD<byte[]>> batchProcessor) {
        logger.info("Creating new Streaming Context");
        val ssc = new JavaStreamingContext(
                JavaSparkContext.fromSparkContext(sparkContext),
                jobArguments.getKinesisReaderBatchDuration()
        );

        // We need to pass a Scala classtag which looks a little ugly in Java.
        JavaDStream<byte[]> kinesisStream = JavaDStream.fromDStream(
                KinesisInputDStream.builder()
                        .streamingContext(ssc)
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
        kinesisStream.foreachRDD(batchProcessor);
        if (jobArguments.isCheckpointEnabled()) {
            ssc.checkpoint(jobArguments.getCheckpointLocation());
        }
        return ssc;
    }

    public void startAndAwaitTermination() throws InterruptedException {
        this.start();
        streamingContext.awaitTermination();
        logger.info("KinesisReader terminated");
    }

    public void start() {
        logger.info("Starting KinesisReader");
        streamingContext.start();
        logger.info("KinesisReader started");
    }

    public void stopGracefully() {
        logger.info("Stopping KinesisReader");
        streamingContext.stop(true, true);
        logger.info("KinesisReader terminated");
    }

}
