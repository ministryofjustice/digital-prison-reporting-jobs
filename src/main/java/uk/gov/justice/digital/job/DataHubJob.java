package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import lombok.val;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import uk.gov.justice.digital.client.kinesis.KinesisStreamingContextProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
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
import java.io.Serializable;
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
public class DataHubJob implements Serializable, Runnable {

    private static final long serialVersionUID = -8033674902036450536L;

    private static final Logger logger = LoggerFactory.getLogger(DataHubJob.class);

    private final RawZone rawZone;
    private final StructuredZoneLoad structuredZoneLoad;
    private final StructuredZoneCDC structuredZoneCDC;
    private final CuratedZoneLoad curatedZoneLoad;
    private final CuratedZoneCDC curatedZoneCDC;
    private final DomainService domainService;
    private final Converter<JavaRDD<Row>, Dataset<Row>> converter;

    private final JobArguments arguments;
    private final JobProperties properties;
    private final SparkSession spark;
    @Inject
    public DataHubJob(
        JobArguments arguments,
        JobProperties properties,
        RawZone rawZone,
        StructuredZoneLoad structuredZoneLoad,
        StructuredZoneCDC structuredZoneCDC,
        CuratedZoneLoad curatedZoneLoad,
        CuratedZoneCDC curatedZoneCDC,
        DomainService domainService,
        @Named("converterForDMS_3_4_6") Converter<JavaRDD<Row>, Dataset<Row>> converter,
        SparkSessionProvider sparkSessionProvider
    ) {
        logger.info("Initializing DataHubJob");


        this.arguments = arguments;
        this.properties = properties;
        this.rawZone = rawZone;
        this.structuredZoneLoad = structuredZoneLoad;
        this.structuredZoneCDC = structuredZoneCDC;
        this.curatedZoneLoad = curatedZoneLoad;
        this.curatedZoneCDC = curatedZoneCDC;
        this.domainService = domainService;
        this.converter = converter;
        String jobName = properties.getSparkJobName();
        SparkConf sparkConf = new SparkConf().setAppName(jobName);
        spark = sparkSessionProvider.getConfiguredSparkSession(sparkConf, arguments.getLogLevel());
        logger.info("DataHubJob initialization complete");
    }

    public static void main(String[] args) {
        logger.info("Job started");
        PicocliRunner.run(DataHubJob.class, MicronautContext.withArgs(args));
    }

    public void batchProcessor(JavaRDD<byte[]> batch) {
        if (batch.isEmpty()) {
            logger.info("Batch: {} - Skipping empty batch", batch.id());
        } else {
            logger.debug("Batch: {} - Processing records", batch.id());

            val startTime = System.currentTimeMillis();

            val rowRdd = batch.map(d -> RowFactory.create(new String(d, StandardCharsets.UTF_8)));
            val dataFrame = converter.convert(rowRdd);

            getTablesInBatch(dataFrame).forEach(tableInfo -> {
                try {
                    val dataFrameForTable = extractDataFrameForSourceTable(dataFrame, tableInfo);
                    dataFrameForTable.persist();

                    rawZone.process(spark, dataFrameForTable, tableInfo);

                    val structuredLoadDataFrame = structuredZoneLoad.process(spark, dataFrameForTable, tableInfo);
                    val structuredIncrementalDataFrame = structuredZoneCDC.process(spark, dataFrameForTable, tableInfo);

                    dataFrameForTable.unpersist();

                    curatedZoneLoad.process(spark, structuredLoadDataFrame, tableInfo);
                    val curatedCdcDataFrame = curatedZoneCDC.process(spark, structuredIncrementalDataFrame, tableInfo);

                    if (!curatedCdcDataFrame.isEmpty()) domainService
                            .refreshDomainUsingDataFrame(spark, curatedCdcDataFrame, tableInfo);

                } catch (Exception e) {
                    logger.error("Caught unexpected exception", e);
                    throw new RuntimeException("Caught unexpected exception", e);
                }
            });

            logger.debug("Batch: {} - Processed records - processed batch in {}ms",
                    batch.id(),
                    System.currentTimeMillis() - startTime
            );
        }
    }

    @Override
    public void run() {
        String jobName = properties.getSparkJobName();
        JavaStreamingContext streamingContext =
                KinesisStreamingContextProvider.buildStreamingContext(arguments, jobName, spark.sparkContext(), this::batchProcessor);
        try {
            streamingContext.start();
            streamingContext.awaitTermination();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                streamingContext.stop(true, true);
                logger.error("Kinesis job interrupted", e);
            } else {
                logger.error("Exception occurred during streaming job", e);
                System.exit(1);
            }
        } finally {
            streamingContext.close();
        }
    }

    private List<Row> getTablesInBatch(Dataset<Row> dataFrame) {
        return dataFrame
                .select(TABLE, SOURCE, OPERATION)
                .dropDuplicates(TABLE, SOURCE)
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
