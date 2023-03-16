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
import uk.gov.justice.digital.job.model.dms.EventRecord;
import uk.gov.justice.digital.zone.RawZone;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.charset.StandardCharsets;

/**
 * Test job to read events from a kinesis data stream and writing to S3 raw zone.
 */
@Singleton
@Command(name = "ArchivePayLoadToRawZoneLoadJob")
public class ArchivePayLoadToRawZoneLoadJob implements Runnable {

    private final KinesisReader kinesisReader;
    private static RawZone rawZone = null;

    @Inject
    public ArchivePayLoadToRawZoneLoadJob(KinesisReader kinesisReader, RawZone rawZone) {
        this.kinesisReader = kinesisReader;
        this.rawZone = rawZone;
    }
    private static final ObjectReader dmsEventReader = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE)
            .readerFor(EventRecord.class);

    public static void main(String[] args) {
        System.out.println("Job started");
        PicocliRunner.run(ArchivePayLoadToRawZoneLoadJob.class);
    }

    private static final VoidFunction<JavaRDD<byte[]>> batchProcessor = batch -> {
        System.out.println("inside batchProcessor.. ");

        val sparkConf = batch.context().getConf();
        sparkConf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                .set("spark.databricks.delta.optimizeWrite.enabled", "true")
                .set("spark.databricks.delta.autoCompact.enabled", "true")
                .set("spark.sql.legacy.charVarcharAsString", "true");

        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        JavaRDD<Row> rowRDD = batch.map((Function<byte[], Row>) msg -> {
            String data = new String(msg, StandardCharsets.UTF_8).replace("(\r\n|\n|\r|\t|\\)", "");
            Row row = RowFactory.create(data);
            return row;
        });

        System.out.println("rowRDD.isEmpty() ==> " + rowRDD.isEmpty());

        Dataset<Row> raw_df = rawZone.process(rowRDD, spark);

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
