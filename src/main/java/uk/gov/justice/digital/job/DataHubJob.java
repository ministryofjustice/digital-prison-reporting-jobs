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
import uk.gov.justice.digital.service.DomainService;
import uk.gov.justice.digital.zone.curated.CuratedZoneCDC;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoad;
import uk.gov.justice.digital.zone.raw.RawZone;
import uk.gov.justice.digital.zone.structured.StructuredZoneCDC;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoad;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.*;

/**
 * Job that reads DMS 3.4.7 load events from a Kinesis stream and processes the data as follows
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
    private final CuratedZoneLoad curatedZoneLoad;
    private final CuratedZoneCDC curatedZoneCDC;
    private final DomainService domainService;
    private final Converter<JavaRDD<Row>, Dataset<Row>> converter;
    private final SparkSessionProvider sparkSessionProvider;

    @Inject
    public DataHubJob(
        KinesisReader kinesisReader,
        RawZone rawZone,
        StructuredZoneLoad structuredZoneLoad,
        StructuredZoneCDC structuredZoneCDC,
        CuratedZoneLoad curatedZoneLoad,
        CuratedZoneCDC curatedZoneCDC,
        DomainService domainService,
        @Named("converterForDMS_3_4_7") Converter<JavaRDD<Row>, Dataset<Row>> converter,
        SparkSessionProvider sparkSessionProvider
    ) {
        logger.info("Initializing DataHubJob");
        this.kinesisReader = kinesisReader;
        this.rawZone = rawZone;
        this.structuredZoneLoad = structuredZoneLoad;
        this.structuredZoneCDC = structuredZoneCDC;
        this.curatedZoneLoad = curatedZoneLoad;
        this.curatedZoneCDC = curatedZoneCDC;
        this.domainService = domainService;
        this.converter = converter;
        this.sparkSessionProvider = sparkSessionProvider;
        logger.info("DataHubJob initialization complete");
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

            getTablesInBatch(dataFrame).forEach(tableInfo -> {
                try {
                    val dataFrameForTable = extractDataFrameForSourceTable(dataFrame, tableInfo);
                    dataFrameForTable.persist();

                    rawZone.process(spark, dataFrameForTable, tableInfo);

                    val structuredLoadDataFrame = structuredZoneLoad.process(spark, dataFrameForTable, tableInfo);
                    curatedZoneLoad.process(spark, structuredLoadDataFrame, tableInfo);

                    val structuredIncrementalDataFrame = structuredZoneCDC.process(spark, dataFrameForTable, tableInfo);
                    val curatedCdcDataFrame = curatedZoneCDC.process(spark, structuredIncrementalDataFrame, tableInfo);

                    if (!curatedCdcDataFrame.isEmpty()) domainService
                            .refreshDomainUsingDataFrame(spark, curatedCdcDataFrame, tableInfo);

                    dataFrameForTable.unpersist();
                } catch (Exception e) {
                    logger.error("Caught unexpected exception while processing table: " + tableInfo.getAs(TABLE), e);
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
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                logger.error("Kinesis job interrupted", e);
            } else {
                logger.error("Exception occurred during streaming job", e);
                System.exit(1);
            }
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
