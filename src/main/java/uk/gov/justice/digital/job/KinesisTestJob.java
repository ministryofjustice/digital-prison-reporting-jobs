package uk.gov.justice.digital.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import uk.gov.justice.digital.client.kinesis.KinesisReader;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.job.model.dms.EventRecord;

/**
 * Test job to explore building a kinesis stream and logging out some metrics.
 */
public class KinesisTestJob {

    private static final ObjectReader dmsEventReader = new ObjectMapper()
        .setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE)
        .registerModule(new JavaTimeModule())
        .readerFor(EventRecord.class);

    public static void main(String[] args) throws Exception {
        KinesisReader kinesisReader = new KinesisReader(JobParameters.fromGlueJob(), batchProcessor);
        kinesisReader.startAndAwaitTermination();
    }

    private static final VoidFunction<JavaRDD<byte[]>> batchProcessor = batch -> {
        if (!batch.isEmpty()) {
            System.out.println("Processing batch: " + batch.id() + " with " + batch.count() + " records");
            batch.foreach((VoidFunction<byte[]>) data -> {
                EventRecord parsedData = dmsEventReader.readValue(data);
                System.out.println("Parsed event has metadata: " + parsedData);
            });
        }
    };

}
