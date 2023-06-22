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
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.zone.CuratedZone;
import uk.gov.justice.digital.zone.RawZone;
import uk.gov.justice.digital.zone.StructuredZoneCDC;
import uk.gov.justice.digital.zone.StructuredZoneLoad;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.*;

/**
 * Job that reads DMS 3.4.6 load events from a Kinesis stream and processes the data as follows
 * - validates the data to ensure it conforms to the expected input format - DPR-341
 * - writes the raw data to the raw zone in s3
 * - validates the data to ensure it confirms to the appropriate table schema
 * - writes this validated data to the structured zone in s3
 */
@Singleton
@Command(name = "DataHubJob")
public class DataHubJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataHubJob.class);


    private final KinesisReader kinesisReader;
    private final RawZone rawZone;
    private final StructuredZoneLoad structuredZoneLoad;
    private final StructuredZoneCDC structuredZoneCDC;
    private final CuratedZone curatedZone;
    private final Converter<JavaRDD<Row>, Dataset<Row>> converter;
    private final SparkSessionProvider sparkSessionProvider;

    @Inject
    public DataHubJob(
        KinesisReader kinesisReader,
        RawZone rawZone,
        StructuredZoneLoad structuredZoneLoad,
        StructuredZoneCDC structuredZoneCDC,
        CuratedZone curatedZone,
        @Named("converterForDMS_3_4_6") Converter<JavaRDD<Row>, Dataset<Row>> converter,
        SparkSessionProvider sparkSessionProvider
    ) {
        this.kinesisReader = kinesisReader;
        this.rawZone = rawZone;
        this.structuredZoneLoad = structuredZoneLoad;
        this.structuredZoneCDC = structuredZoneCDC;
        this.curatedZone = curatedZone;
        this.converter = converter;
        this.sparkSessionProvider = sparkSessionProvider;
    }

    public static void main(String[] args) {
        logger.info("Job started");
        PicocliRunner.run(DataHubJob.class, MicronautContext.withArgs(args));
    }

    private void batchProcessor(JavaRDD<byte[]> batch) {
        if (batch.isEmpty()) {
            logger.info("Batch: {} - Skipping empty batch", batch.id());
        }
        else {
            val batchCount = batch.count();

            logger.info("Batch: {} - Processing {} records", batch.id(), batchCount);

            val startTime = System.currentTimeMillis();

            val spark = sparkSessionProvider.getConfiguredSparkSession(batch.context().getConf());
            val rowRdd = batch.map(d -> RowFactory.create(new String(d, StandardCharsets.UTF_8)));
            val dataFrame = converter.convert(rowRdd);

            getTablesInBatch(dataFrame).forEach(table -> {
                try {
                    val dataFrameForTable = extractDataFrameForSourceTable(dataFrame, table);
                    dataFrameForTable.persist();

                    rawZone.process(spark, dataFrameForTable, table);

                    val structuredLoadDataFrame = structuredZoneLoad.process(spark, dataFrameForTable, table);
                    curatedZone.process(spark, structuredLoadDataFrame, table);

                    val structuredIncrementalDataFrame = structuredZoneCDC.process(spark, dataFrameForTable, table);
                    curatedZone.process(spark, structuredIncrementalDataFrame, table);
                } catch (Exception e) {
                    logger.error("Caught unexpected exception", e);
                    throw new RuntimeException("Caught unexpected exception", e);
                }
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
            logger.error("Kinesis job interrupted");
            throw new RuntimeException(e);
        }
    }

    private List<Row> getTablesInBatch(Dataset<Row> dataFrame) {
        return dataFrame
                .select(TABLE, SOURCE, OPERATION)
                .distinct()
                .collectAsList();
    }

    private Dataset<Row> extractDataFrameForSourceTable(Dataset<Row> dataFrame, Row row) {
        final String source = row.getAs(SOURCE);
        final String table = row.getAs(TABLE);
        return (dataFrame == null) ? null
                : dataFrame
                .filter(col(SOURCE).equalTo(source).and(col(TABLE).equalTo(table)))
                .orderBy(col(TIMESTAMP));
    }
}
