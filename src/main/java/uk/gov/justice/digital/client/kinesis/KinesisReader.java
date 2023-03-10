package uk.gov.justice.digital.client.kinesis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisInitialPositions;
import org.apache.spark.streaming.kinesis.KinesisInputDStream;
import scala.reflect.ClassTag$;
import uk.gov.justice.digital.config.Config;

public class KinesisReader {

    private final JavaStreamingContext streamingContext;
    private final JavaDStream<byte[]> kinesisStream;

    public KinesisReader() {
        String jobName = Config.getJobName();

        streamingContext = new JavaStreamingContext(
            new SparkConf().setAppName(jobName),
            Config.getKinesisReaderBatchDuration()
        );

        // Create kinesis stream and print out simple metrics.
        kinesisStream = JavaDStream.fromDStream(
            KinesisInputDStream.builder()
                .streamingContext(streamingContext)
                .endpointUrl(Config.getAwsKinesisEndpointUrl())
                .regionName(Config.getAwsRegion())
                .streamName(Config.getKinesisReaderStreamName())
                .initialPosition(new KinesisInitialPositions.TrimHorizon())
                .checkpointAppName(jobName)
                .build(),
            // We need to pass a Scala classtag which looks a little ugly in Java.
            ClassTag$.MODULE$.apply(byte[].class)
        );

    }

    public KinesisReader(VoidFunction<JavaRDD<byte[]>> batchProcessor) {
        this();
        kinesisStream.foreachRDD(batchProcessor);
    }

    public JavaDStream<byte[]> getKinesisStream() {
        return kinesisStream;
    }

    public void startAndAwaitTermination() throws InterruptedException {
        streamingContext.start();
        System.out.println("KinesisReader started");
        streamingContext.awaitTermination();
        System.out.println("KinesisReader terminated");
    }

}
