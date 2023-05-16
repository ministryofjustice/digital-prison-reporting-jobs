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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassTag$;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.config.JobProperties;

@Bean
public class KinesisReader {

    private static final Logger logger = LoggerFactory.getLogger(KinesisReader.class);

    private final JavaStreamingContext streamingContext;
    private final JavaDStream<byte[]> kinesisStream;

    @Inject
    public KinesisReader(JobParameters jobParameters,
                         JobProperties jobProperties) {
        String jobName = jobProperties.getSparkJobName();

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

        logger.info("Configuration - endpointUrl: {} awsRegion: {} streamName: {} batchDuration: {}",
                jobParameters.getAwsKinesisEndpointUrl(),
                jobParameters.getAwsRegion(),
                jobParameters.getKinesisReaderStreamName(),
                jobParameters.getKinesisReaderBatchDuration()
        );

    }

    public void setBatchProcessor(VoidFunction<JavaRDD<byte[]>> batchProcessor) {
        kinesisStream.foreachRDD(batchProcessor);
    }

    public void startAndAwaitTermination() throws InterruptedException {
        streamingContext.start();
        logger.info("KinesisReader started");
        streamingContext.awaitTermination();
        logger.info("KinesisReader terminated");
    }

}