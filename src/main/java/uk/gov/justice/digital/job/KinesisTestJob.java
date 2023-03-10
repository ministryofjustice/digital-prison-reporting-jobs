package uk.gov.justice.digital.job;

import com.amazonaws.services.kinesis.model.Record;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisInitialPositions;
import org.apache.spark.streaming.kinesis.KinesisInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.reflect.ClassTag$;

import java.nio.charset.StandardCharsets;

/**
 * Test job to explore building a kinesis stream and logging out some metrics.
 */
public class KinesisTestJob {

    private static final String REGION = "eu-west-2";
    private static final String APP_NAME = "kinesis-test-job";
    private static final String ENDPOINT_URL = "https://kinesis.eu-west-2.amazonaws.com";
    private static final String STREAM_NAME = "dpr-319-kinesis-test-stream";

    public static void main(String[] args) throws Exception {

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName(APP_NAME);
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        // TODO - replace with identity - iirc this is available in Java8
        //      - is there a better way to get a DStream for Record?
        Function1<Record, Record> processor = (Record record) -> record;

        // Create kinesis stream and print out simple metrics.
        JavaDStream<byte[]> kinesisStream = JavaDStream.fromDStream(
            KinesisInputDStream.builder()
                .streamingContext(streamingContext)
                .endpointUrl(ENDPOINT_URL)
                .regionName(REGION)
                .streamName(STREAM_NAME)
                // TODO - review this setting - consider making this configurable
                .initialPosition(new KinesisInitialPositions.TrimHorizon())
                .checkpointAppName(APP_NAME)
                .build(),
//                .buildWithMessageHandler(processor, ClassTag$.MODULE$.apply(Record.class)),
            ClassTag$.MODULE$.apply(byte[].class)
        );

        kinesisStream.foreachRDD((VoidFunction<JavaRDD<byte[]>>) batch -> {
            if (!batch.isEmpty()) {
                System.out.println("Processing batch: " + batch.id() + " with " + batch.count() + " records");
                batch.foreach((VoidFunction<byte[]>) bytes ->
                    System.out.println("Got record: " + new String(bytes, StandardCharsets.UTF_8))
                );
            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

}
