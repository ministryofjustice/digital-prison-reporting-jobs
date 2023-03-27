package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import lombok.val;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import uk.gov.justice.digital.client.kinesis.KinesisReader;
import uk.gov.justice.digital.zone.RawZone;
import uk.gov.justice.digital.zone.StructuredZone;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.charset.StandardCharsets;

import static org.apache.spark.sql.functions.*;
import static uk.gov.justice.digital.job.model.Columns.*;

/**
 * Job that reads DMS 3.4.6 load events from a Kinesis stream and processes the data as follows
 *  - TODO - validates the data to ensure it conforms to the expected input format - DPR-341
 *  - writes the raw data to the raw zone in s3
 *  - TODO - validates the data to ensure it confirms to the appropriate table schema
 *  - TODO - writes this validated data to the structured zone in s3
 */
@Singleton
@Command(name = "DataHubJob")
public class DataHubJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataHubJob.class);

    private final KinesisReader kinesisReader;
    private final RawZone rawZone;
    private final StructuredZone structuredZone;

    @Inject
    public DataHubJob(KinesisReader kinesisReader, RawZone rawZone, StructuredZone structuredZone) {
        this.kinesisReader = kinesisReader;
        this.rawZone = rawZone;
        this.structuredZone = structuredZone;
    }

    public static void main(String[] args) {
        logger.info("Job started");
        PicocliRunner.run(DataHubJob.class);
    }

    private void batchProcessor(JavaRDD<byte[]> batch) {
        if (!batch.isEmpty()) {
            logger.info("Batch: {} - Processing {} records", batch.id(), batch.count());

            val startTime = System.currentTimeMillis();

            val spark = getConfiguredSparkSession(batch.context().getConf());

            val rowRdd = batch.map(d -> RowFactory.create(new String(d, StandardCharsets.UTF_8)));

            Dataset<Row> dataFrame = fromRawDMS_3_4_6(rowRdd, spark);

            rawZone.process(dataFrame);

            structuredZone.process(dataFrame);

            logger.info("Batch: {} - Processed {} records - processed batch in {}ms",
                batch.id(),
                batch.count(),
                System.currentTimeMillis() - startTime
            );
        }
    };

    // TODO - extract this - see DPR-341
    public Dataset<Row> fromRawDMS_3_4_6(JavaRDD<Row> rowRDD, SparkSession spark) {

        val eventsSchema = new StructType()
            .add(DATA, DataTypes.StringType);

        return spark
            .createDataFrame(rowRDD, eventsSchema)
            .withColumn(JSON_DATA, col(DATA))
            .withColumn(DATA, get_json_object(col(JSON_DATA), "$.data"))
            .withColumn(METADATA, get_json_object(col(JSON_DATA), "$.metadata"))
            .withColumn(SOURCE, lower(get_json_object(col(METADATA), "$.schema-name")))
            .withColumn(TABLE, lower(get_json_object(col(METADATA), "$.table-name")))
            .withColumn(OPERATION, lower(get_json_object(col(METADATA), "$.operation")))
            .drop(JSON_DATA);
    }

    private static SparkSession getConfiguredSparkSession(SparkConf sparkConf) {
        sparkConf
            .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .set("spark.databricks.delta.optimizeWrite.enabled", "true")
            .set("spark.databricks.delta.autoCompact.enabled", "true")
            .set("spark.sql.legacy.charVarcharAsString", "true");

        return SparkSession.builder()
            .config(sparkConf)
            .getOrCreate();
    }

    @Override
    public void run() {
        try {
            kinesisReader.setBatchProcessor(this::batchProcessor);
            kinesisReader.startAndAwaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
