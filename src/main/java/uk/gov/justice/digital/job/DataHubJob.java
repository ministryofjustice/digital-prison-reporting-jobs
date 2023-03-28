package uk.gov.justice.digital.job;

import lombok.val;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import uk.gov.justice.digital.client.kinesis.KinesisReader;
import uk.gov.justice.digital.zone.CuratedZone;
import uk.gov.justice.digital.zone.RawZone;
import uk.gov.justice.digital.zone.StructuredZone;
import io.micronaut.configuration.picocli.PicocliRunner;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.get_json_object;
import static uk.gov.justice.digital.job.model.Columns.DATA;
import static uk.gov.justice.digital.job.model.Columns.METADATA;
import static uk.gov.justice.digital.job.model.Columns.SOURCE;
import static uk.gov.justice.digital.job.model.Columns.TABLE;
import static uk.gov.justice.digital.job.model.Columns.JSON_DATA;
import static uk.gov.justice.digital.job.model.Columns.OPERATION;

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
    private final CuratedZone curatedZone;

    @Inject
    public DataHubJob(KinesisReader kinesisReader, RawZone rawZone, StructuredZone structuredZone, CuratedZone curatedZone) {
        this.kinesisReader = kinesisReader;
        this.rawZone = rawZone;
        this.structuredZone = structuredZone;
        this.curatedZone = curatedZone;
    }

    public static void main(String[] args) {
        logger.info("DataHub Job started..");
        PicocliRunner.run(DataHubJob.class);
    }

    private void batchProcessor(JavaRDD<byte[]> batch) {
        if (!batch.isEmpty()) {
            logger.info("Batch: {} - Processing {} records", batch.id(), batch.count());

            val startTime = System.currentTimeMillis();

            val spark = getConfiguredSparkSession(batch.context().getConf());

            val rowRdd = batch.map(d -> RowFactory.create(new String(d, StandardCharsets.UTF_8)));

            Dataset<Row> dataFrame = fromRawDMS_3_4_6(rowRdd, spark);

            getTablesWithLoadRecords(dataFrame).forEach(row -> {
                val rawDataFrame = rawZone.process(dataFrame, row);

                val validStructuredDataFrame = structuredZone.process(rawDataFrame, row);

                curatedZone.process(validStructuredDataFrame, row);

            });

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

    private List<Row> getTablesWithLoadRecords(Dataset<Row> dataFrame) {
        return dataFrame
                .filter(col(OPERATION).equalTo("load"))
                .select(TABLE, SOURCE, OPERATION)
                .distinct()
                .collectAsList();
    }
}
