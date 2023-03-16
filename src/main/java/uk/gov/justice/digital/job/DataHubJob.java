package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import lombok.val;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import uk.gov.justice.digital.client.kinesis.KinesisReader;
import uk.gov.justice.digital.zone.RawZone;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.charset.StandardCharsets;

/**
 * Test job to read events from a kinesis data stream and writing to S3 raw zone.
 */
@Singleton
@Command(name = "DataHubJob")
public class DataHubJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataHubJob.class);

    private final KinesisReader kinesisReader;
    private final RawZone rawZone;

    @Inject
    public DataHubJob(KinesisReader kinesisReader, RawZone rawZone) {
        this.kinesisReader = kinesisReader;
        this.rawZone = rawZone;
    }

    public RawZone getRawZone() {
        return rawZone;
    }
    public static void main(String[] args) {
        logger.info("Job started");
        PicocliRunner.run(DataHubJob.class);
    }

    private final VoidFunction<JavaRDD<byte[]>> batchProcessor = batch -> {
        logger.info("inside batchProcessor.. ");

        SparkSession spark = getSparkSession(batch);

        JavaRDD<Row> rowRDD = batch.map((Function<byte[], Row>) msg -> RowFactory.create(new String(msg, StandardCharsets.UTF_8)));

        logger.info("rowRDD.isEmpty() ==> " + rowRDD.isEmpty());

        Dataset<Row> rawDataFrame = getRawZone().process(rowRDD, spark);

    };

    private static SparkSession getSparkSession(JavaRDD<byte[]> batch) {
        val sparkConf = batch.context().getConf();
        sparkConf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                .set("spark.databricks.delta.optimizeWrite.enabled", "true")
                .set("spark.databricks.delta.autoCompact.enabled", "true")
                .set("spark.sql.legacy.charVarcharAsString", "true");

        return SparkSession.builder().config(sparkConf).getOrCreate();
    }

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
