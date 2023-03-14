package uk.gov.justice.digital.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import lombok.val;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import uk.gov.justice.digital.client.kinesis.KinesisReader;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.job.model.dms.EventRecord;

import java.time.Instant;

/**
 * Test job to explore building a kinesis stream and logging out some metrics.
 */
public class KinesisTestJob {

    private static final ObjectReader dmsEventReader = new ObjectMapper()
        .setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE)
        .readerFor(EventRecord.class);

    public static void main(String[] args) throws Exception {
        KinesisReader kinesisReader = new KinesisReader(JobParameters.fromGlueJob(), batchProcessor);
        kinesisReader.startAndAwaitTermination();
    }

    private static final VoidFunction<JavaRDD<byte[]>> batchProcessor = batch -> {
        if (!batch.isEmpty()) {
            System.out.println("Batch: " + batch.id() + " - Processing " + batch.count() + " records");

            val result = batch
                .map((Function<byte[], EventRecord>) dmsEventReader::readValue)
                .filter((record) -> record.getMetadata().getOperation().equals("load"))
                .map((Function<EventRecord, Long>) record -> {
                    Instant timestamp = Instant.parse(record.getMetadata().getTimestamp());
                    return Instant.now().toEpochMilli() - timestamp.toEpochMilli();
                });

            val timings = result.collect();

            // Compute basic stats
            val min = timings.stream().min(Long::compare);
            val max = timings.stream().max(Long::compare);
            val average = timings.stream().reduce(0L, Long::sum) / timings.size();

            System.out.println("Batch: " + batch.id() +
                " - Processed " + result.count() + " records" +
                " - skipped " + (batch.count() - result.count()) + " records" +
                " - timings min: " + min.map(Object::toString).orElse("UNKNOWN") + "ms" +
                " max: " + max.map(Object::toString).orElse("UNKNOWN") + "ms" +
                " average: " + average + "ms"
            );
        }
    };

}
