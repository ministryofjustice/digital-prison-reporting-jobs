package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import lombok.val;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import uk.gov.justice.digital.client.kinesis.KinesisReader;
import uk.gov.justice.digital.zone.RawZone;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.charset.StandardCharsets;

import static org.apache.spark.sql.functions.*;

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

        logger.info("rowRDD.isEmpty(): " + rowRDD.isEmpty());
        if (!rowRDD.isEmpty()) {
            Dataset<Row> dmsDataFrame = fromRawDMS_3_4_6(rowRDD, spark);

            getRawZone().process(dmsDataFrame);

            // Structured zone uses this dmsDataFrame
        }

    };

    public Dataset<Row> fromRawDMS_3_4_6(JavaRDD<Row> rowRDD, SparkSession spark) {
        logger.info("Preparing Raw DMS Dataframe..");

        StructType evemtsSchema = new StructType()
                .add("data", DataTypes.StringType);

        Dataset<Row> df = spark.createDataFrame(rowRDD, evemtsSchema);
        Dataset<Row> df2 = df.withColumn("jsonData", col("data").cast("string"))
                .withColumn("data", get_json_object(col("jsonData"), "$.data"))
                .withColumn("metadata", get_json_object(col("jsonData"), "$.metadata"))
                .withColumn("source", lower(get_json_object(col("metadata"), "$.schema-name")))
                .withColumn("table", lower(get_json_object(col("metadata"), "$.table-name")))
                .withColumn("operation", lower(get_json_object(col("metadata"), "$.operation")))
                .drop("jsonData");
        return df2;
    }


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
