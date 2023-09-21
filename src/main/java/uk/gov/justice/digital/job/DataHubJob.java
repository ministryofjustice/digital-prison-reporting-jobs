package uk.gov.justice.digital.job;

import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.Job$;
import io.micronaut.configuration.picocli.PicocliRunner;
import lombok.val;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.SOURCE;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.TABLE;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.TIMESTAMP;

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

    private static final URI stopFile = URI.create("s3://dpr-working-test/stopFile");

    private final RawZone rawZone;
    private final StructuredZoneLoad structuredZoneLoad;
    private final StructuredZoneCDC structuredZoneCDC;
    private final CuratedZoneLoad curatedZoneLoad;
    private final CuratedZoneCDC curatedZoneCDC;
    private final DomainService domainService;
    private final Converter<JavaRDD<Row>, Dataset<Row>> converter;

    private final JobArguments arguments;
    private final JobProperties properties;
    private final SparkSessionProvider sparkSessionProvider;
    // SparkSession object requires special handling for Spark checkpointing since it can not be deserialized
    // in a working state from a checkpoint.
    // Transient ensures Java does not attempt to serialize it.
    // Volatile ensures it keeps the 'final' concurrency initialization guarantee.
    private transient volatile SparkSession spark;

    private Job$ job;
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
        this.sparkSessionProvider = sparkSessionProvider;
        String jobName = properties.getSparkJobName();
        SparkConf sparkConf = jobArgumentsToSparkConf(arguments, jobName);
        spark = sparkSessionProvider.getConfiguredSparkSession(sparkConf, arguments.getLogLevel(), arguments.isCheckpointEnabled());
        logger.info("DataHubJob initialization complete");
    }

    // todo sort this out
    public static SparkConf jobArgumentsToSparkConf(JobArguments arguments, String sparkJobName) {
        return new SparkConf()
                .setAppName(sparkJobName)
                .set("spark.streaming.kinesis.retry.waitTime", arguments.getKinesisReaderRetryWaitTime())
                .set("spark.streaming.kinesis.retry.maxAttempts", Integer.toString(arguments.getKinesisReaderRetryMaxAttempts()));
    }

    public static void main(String[] args) {
        logger.info("Job started");
        PicocliRunner.run(DataHubJob.class, MicronautContext.withArgs(args));
    }

    private boolean shouldWeStop() throws IOException {
        FileSystem fs = FileSystem.get(stopFile, spark.sparkContext().hadoopConfiguration());
        return !fs.exists(new Path(stopFile));
    }

    public void batchProcessor(JavaRDD<byte[]> batch) throws IOException {
        if(shouldWeStop()) {
            logger.info("Batch cancelled: Time to stop");
            job.commit();
            logger.info("Batch cancelled: gracefully stopped, exiting");
            System.exit(0);
        }
        if (batch.isEmpty()) {
            logger.info("Batch: {} - Skipping empty batch", batch.id());
        } else {
            logger.info("Batch: {} - Processing records", batch.id());

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
        GlueContext glueContext = new GlueContext(streamingContext.sparkContext());
        job = Job$.MODULE$.init(jobName, glueContext, arguments.getConfig());
        try {
            streamingContext.start();
            streamingContext.awaitTermination();
            streamingContext.close();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                streamingContext.stop(true, true);
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

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        String jobName = properties.getSparkJobName();
        SparkConf sparkConf = jobArgumentsToSparkConf(arguments, jobName);
        this.spark = sparkSessionProvider.getConfiguredSparkSession(sparkConf, arguments.getLogLevel(), arguments.isCheckpointEnabled());
    }
}
