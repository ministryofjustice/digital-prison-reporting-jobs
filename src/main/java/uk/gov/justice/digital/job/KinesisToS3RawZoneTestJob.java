package uk.gov.justice.digital.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import io.micronaut.configuration.picocli.PicocliRunner;
import lombok.val;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;

import picocli.CommandLine.Command;
import uk.gov.justice.digital.client.kinesis.KinesisReader;
import uk.gov.justice.digital.config.Properties;
import uk.gov.justice.digital.job.model.dms.EventRecord;
import uk.gov.justice.digital.util.SparkUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Instant;
import java.util.List;

/**
 * Test job to read events from a kinesis stream and writing to S3 raw zone.
 */
@Singleton
@Command(name = "KinesisToS3RawZoneTestJob")
public class KinesisToS3RawZoneTestJob implements Runnable {

    private static String PREFIX = "s3://dpr-297-raw-zone/raw/";
    private static String DELTA_FORMAT = "delta";
    private static SparkSession spark = null;
    private final KinesisReader kinesisReader;

    @Inject
    public KinesisToS3RawZoneTestJob(KinesisReader kinesisReader) {
        this.kinesisReader = kinesisReader;
    }
    private static final ObjectReader dmsEventReader = new ObjectMapper()
        .setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE)
        .readerFor(EventRecord.class);


    public static void main(String[] args) {
        spark = SparkUtils.getSparkSession(SparkUtils.getSparkConf(Properties.getSparkJobName()));

        System.out.println("Job started");
        PicocliRunner.run(KinesisTestJob.class);
    }

    private static String getTablePath(final String prefix, final String schema, final String table, String operation) {
        return prefix + "/" + schema + "/" + table + "/" + operation;
    }

    private static final VoidFunction<JavaRDD<byte[]>> batchProcessor = batch -> {
        System.out.println("Batch is empty?: " + batch.isEmpty());

        if (!batch.isEmpty()) {
            System.out.println("Batch: " + batch.id() + " - Processing " + batch.count() + " records");

            val result = batch
                .map((Function<byte[], EventRecord>) dmsEventReader::readValue)
                .filter((record) -> record.getMetadata().getOperation().equals("load"))
                .map((Function<EventRecord, Long>) record -> {

                    val source = record.getMetadata().getSchemaName();
                    val table  = record.getMetadata().getTableName();
                    val operation  = record.getMetadata().getOperation();

                    System.out.println("source : " +  source + " table : " + table + " operation =  " + operation);

                    val dataFrame = spark.createDataFrame((List<?>) record, EventRecord.class);

                    System.out.println("dataFrame.count() : " +  dataFrame.count());

                    dataFrame.write()
                            .format(DELTA_FORMAT)
                            .mode(SaveMode.Append)
                            .option("path", getTablePath(PREFIX, source, table, operation))
                            .save();

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

    @Override
    public void run() {
        try {
            kinesisReader.setBatchProcessor(batchProcessor);
            kinesisReader.startAndAwaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
