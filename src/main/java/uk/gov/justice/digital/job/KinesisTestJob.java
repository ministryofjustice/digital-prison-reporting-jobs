package uk.gov.justice.digital.job;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import uk.gov.justice.digital.client.kinesis.KinesisReader;

import java.nio.charset.StandardCharsets;

/**
 * Test job to explore building a kinesis stream and logging out some metrics.
 */
public class KinesisTestJob {

    private static final VoidFunction<JavaRDD<byte[]>> batchProcessor = batch -> {
        if (!batch.isEmpty()) {
            System.out.println("Processing batch: " + batch.id() + " with " + batch.count() + " records");
            batch.foreach((VoidFunction<byte[]>) data -> {
                // TODO - parse data and determine lag.
                System.out.println("Got record: " + new String(data, StandardCharsets.UTF_8));
            });
        }
    };

    public static void main(String[] args) throws Exception {
        KinesisReader kinesisReader = new KinesisReader(batchProcessor);
        kinesisReader.startAndAwaitTermination();
    }

}
