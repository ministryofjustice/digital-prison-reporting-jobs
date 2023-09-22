package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import lombok.val;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import uk.gov.justice.digital.client.kinesis.KinesisReader;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.converter.Converter;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.DomainService;
import uk.gov.justice.digital.service.SourceReferenceService;
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
import static org.apache.spark.sql.functions.lit;
import static uk.gov.justice.digital.common.CommonDataFields.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
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


    public final KinesisReader kinesisReader;
    private final RawZone rawZone;
    private final StructuredZoneLoad structuredZoneLoad;
    private final StructuredZoneCDC structuredZoneCDC;
    private final CuratedZoneLoad curatedZoneLoad;
    private final CuratedZoneCDC curatedZoneCDC;
    private final DomainService domainService;
    private final SourceReferenceService sourceReferenceService;
    private final DataStorageService storageService;
    private final Converter<JavaRDD<Row>, Dataset<Row>> converter;
    private final String violationsPath;
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
            SourceReferenceService sourceReferenceService,
            DataStorageService storageService,
            @Named("converterForDMS_3_4_7") Converter<JavaRDD<Row>, Dataset<Row>> converter,
            SparkSessionProvider sparkSessionProvider
    ) {
        logger.info("Initializing DataHubJob");
        String jobName = properties.getSparkJobName();
        SparkConf sparkConf = new SparkConf().setAppName(jobName);

        this.spark = sparkSessionProvider.getConfiguredSparkSession(sparkConf, arguments.getLogLevel());
        this.kinesisReader = new KinesisReader(arguments, jobName, spark.sparkContext());
        this.rawZone = rawZone;
        this.structuredZoneLoad = structuredZoneLoad;
        this.structuredZoneCDC = structuredZoneCDC;
        this.curatedZoneLoad = curatedZoneLoad;
        this.curatedZoneCDC = curatedZoneCDC;
        this.domainService = domainService;
        this.sourceReferenceService = sourceReferenceService;
        this.storageService = storageService;
        this.converter = converter;
        this.violationsPath = arguments.getViolationsS3Path();
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

                    String sourceName = tableInfo.getAs(SOURCE);
                    String tableName = tableInfo.getAs(TABLE);

                    val optionalSourceReference = sourceReferenceService.getSourceReference(sourceName, tableName);

                    if (optionalSourceReference.isPresent()) {
                        val sourceReference = optionalSourceReference.get();

                        rawZone.process(spark, dataFrameForTable, sourceReference);

                        val structuredLoadDataFrame = structuredZoneLoad.process(spark, dataFrameForTable, sourceReference);
                        val structuredIncrementalDataFrame = structuredZoneCDC.process(spark, dataFrameForTable, sourceReference);

                        dataFrameForTable.unpersist();

                        curatedZoneLoad.process(spark, structuredLoadDataFrame, sourceReference);
                        val curatedCdcDataFrame = curatedZoneCDC.process(spark, structuredIncrementalDataFrame, sourceReference);

                        if (!curatedCdcDataFrame.isEmpty()) domainService
                                .refreshDomainUsingDataFrame(
                                        spark,
                                        curatedCdcDataFrame,
                                        sourceReference.getSource(),
                                        sourceReference.getTable()
                                );
                    } else {
                        handleNoSchemaFound(spark, dataFrame, sourceName, tableName);
                    }

                } catch (Exception e) {
                    logger.error("Caught unexpected exception", e);
                    throw new RuntimeException("Caught unexpected exception", e);
                }

                logger.debug("Batch: {} - Processed records - processed batch in {}ms",
                        batch.id(),
                        System.currentTimeMillis() - startTime
                );
            });

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

    private void handleNoSchemaFound(
            SparkSession spark,
            Dataset<Row> dataFrame,
            String source,
            String table
    ) throws DataStorageException {
        logger.warn("Violation - No schema found for {}/{}", source, table);
        val destinationPath = createValidatedPath(violationsPath, source, table);

        val missingSchemaRecords = dataFrame
                .select(col(DATA), col(METADATA))
                .withColumn(ERROR, lit(String.format("Schema does not exist for %s/%s", source, table)))
                .drop(OPERATION);

        storageService.append(destinationPath, missingSchemaRecords);
        storageService.updateDeltaManifestForTable(spark, destinationPath);
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
        return dataFrame
                .filter(col(SOURCE).equalTo(source).and(col(TABLE).equalTo(table)))
                .orderBy(col(TIMESTAMP));
    }
}
