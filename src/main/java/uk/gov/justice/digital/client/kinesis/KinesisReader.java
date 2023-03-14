package uk.gov.justice.digital.client.kinesis;

import io.micronaut.context.annotation.Bean;
import jakarta.inject.Inject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisInitialPositions;
import org.apache.spark.streaming.kinesis.KinesisInputDStream;
import scala.reflect.ClassTag$;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.config.Properties;

@Bean
public class KinesisReader {

    private final JavaStreamingContext streamingContext;
    private final JavaDStream<byte[]> kinesisStream;

    @Inject
    public KinesisReader(JobParameters jobParameters) {
        String jobName = Properties.getSparkJobName();

        streamingContext = new JavaStreamingContext(
            new SparkConf().setAppName(jobName),
            jobParameters.getKinesisReaderBatchDuration()
        );

        kinesisStream = JavaDStream.fromDStream(
            KinesisInputDStream.builder()
                .streamingContext(streamingContext)
                .endpointUrl(jobParameters.getAwsKinesisEndpointUrl())
                .regionName(jobParameters.getAwsRegion())
                .streamName(jobParameters.getKinesisReaderStreamName())
                .initialPosition(new KinesisInitialPositions.TrimHorizon())
                .checkpointAppName(jobName)
                .build(),
            // We need to pass a Scala classtag which looks a little ugly in Java.
            ClassTag$.MODULE$.apply(byte[].class)
        );

        System.out.println("KinesisReader configuration" +
            " - endpointUrl: " + jobParameters.getAwsKinesisEndpointUrl() +
            " - aws region: " + jobParameters.getAwsRegion() +
            " - stream name: " + jobParameters.getKinesisReaderStreamName() +
            " - batch duration: " + jobParameters.getKinesisReaderBatchDuration()
        );

    }

    public JavaDStream<byte[]> getKinesisStream() {
        return kinesisStream;
    }

    public void setBatchProcessor(VoidFunction<JavaRDD<byte[]>> batchProcessor) {
        kinesisStream.foreachRDD(batchProcessor);
    }

    public void startAndAwaitTermination() throws InterruptedException {
        streamingContext.start();
        System.out.println("KinesisReader started");
        streamingContext.awaitTermination();
        System.out.println("KinesisReader terminated");
    }

}
