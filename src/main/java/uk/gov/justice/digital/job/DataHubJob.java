package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import lombok.val;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import uk.gov.justice.digital.client.kinesis.KinesisReader;
import uk.gov.justice.digital.converter.Converter;
import uk.gov.justice.digital.zone.CuratedZone;
import uk.gov.justice.digital.zone.RawZone;
import uk.gov.justice.digital.zone.StructuredZone;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.nio.charset.StandardCharsets;
import java.util.List;
import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.job.model.Columns.*;

/**
 * Job that reads DMS 3.4.6 load events from a Kinesis stream and processes the data as follows
 *  - validates the data to ensure it conforms to the expected input format - DPR-341
 *  - writes the raw data to the raw zone in s3
 *  - validates the data to ensure it confirms to the appropriate table schema
 *  - writes this validated data to the structured zone in s3
 */
@Singleton
@Command(name = "DataHubJob")
public class DataHubJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataHubJob.class);

    private final KinesisReader kinesisReader;
    private final RawZone rawZone;
    private final StructuredZone structuredZone;
    private final CuratedZone curatedZone;
    private final Converter converter;
    private final SparkSessionProvider sparkSessionProvider;

    @Inject
    public DataHubJob(
        KinesisReader kinesisReader,
        RawZone rawZone,
        StructuredZone structuredZone,
        CuratedZone curatedZone,
        @Named("converterForDMS_3_4_6") Converter converter,
        SparkSessionProvider sparkSessionProvider
    ) {
        this.kinesisReader = kinesisReader;
        this.rawZone = rawZone;
        this.structuredZone = structuredZone;
        this.curatedZone = curatedZone;
        this.converter = converter;
        this.sparkSessionProvider = sparkSessionProvider;
    }

    public static void main(String[] args) {
        logger.info("Job started");
        PicocliRunner.run(DataHubJob.class);
    }

    private void batchProcessor(JavaRDD<byte[]> batch) {
        if (!batch.isEmpty()) {
            val batchCount = batch.count();

            logger.info("Batch: {} - Processing {} records", batch.id(), batchCount);

            val startTime = System.currentTimeMillis();

            val spark = sparkSessionProvider.getConfiguredSparkSession(batch.context().getConf());
            val rowRdd = batch.map(d -> RowFactory.create(new String(d, StandardCharsets.UTF_8)));
            val dataFrame = converter.convert(rowRdd, spark);

            getTablesWithLoadRecords(dataFrame).forEach(table -> {
                val rawDataFrame = rawZone.process(spark, dataFrame, table);
                val structuredDataFrame = structuredZone.process(spark, rawDataFrame, table);
                curatedZone.process(spark, structuredDataFrame, table);
            });

            logger.info("Batch: {} - Processed {} records - processed batch in {}ms",
                    batch.id(),
                    batchCount,
                    System.currentTimeMillis() - startTime
            );
        }
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
