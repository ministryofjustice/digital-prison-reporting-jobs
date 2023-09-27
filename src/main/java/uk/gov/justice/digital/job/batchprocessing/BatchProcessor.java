package uk.gov.justice.digital.job.batchprocessing;

import jakarta.inject.Inject;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.converter.Converter;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.DomainService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.zone.curated.CuratedZoneCDC;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoad;
import uk.gov.justice.digital.zone.raw.RawZone;
import uk.gov.justice.digital.zone.structured.StructuredZoneCDC;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoad;

import javax.inject.Singleton;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static uk.gov.justice.digital.common.CommonDataFields.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.*;
/**
 * Responsible for providing a BatchProcessor.
 * Dependencies which rely on the SparkSession are manually injected to ensure spark contexts/sessions/etc.
 * are created just once with a common configuration.
 */
@Singleton
public class BatchProcessor {

    private static final Logger logger = LoggerFactory.getLogger(BatchProcessor.class);

    private final RawZone rawZone;
    private final StructuredZoneLoad structuredZoneLoad;
    private final StructuredZoneCDC structuredZoneCDC;
    private final CuratedZoneLoad curatedZoneLoad;
    private final CuratedZoneCDC curatedZoneCDC;
    private final DomainService domainService;
    private final SourceReferenceService sourceReferenceService;
    private final DataStorageService storageService;
    private final String violationsPath;
    @Inject
    public BatchProcessor(
            JobArguments arguments,
            RawZone rawZone,
            StructuredZoneLoad structuredZoneLoad,
            StructuredZoneCDC structuredZoneCDC,
            CuratedZoneLoad curatedZoneLoad,
            CuratedZoneCDC curatedZoneCDC,
            DomainService domainService,
            SourceReferenceService sourceReferenceService,
            DataStorageService storageService
    ) {
        logger.info("Initializing BatchProcessorProvider");
        this.rawZone = rawZone;
        this.structuredZoneLoad = structuredZoneLoad;
        this.structuredZoneCDC = structuredZoneCDC;
        this.curatedZoneLoad = curatedZoneLoad;
        this.curatedZoneCDC = curatedZoneCDC;
        this.domainService = domainService;
        this.sourceReferenceService = sourceReferenceService;
        this.storageService = storageService;
        this.violationsPath = arguments.getViolationsS3Path();
        logger.info("BatchProcessorProvider initialization complete");
    }

    public void processBatch(SparkSession spark, Converter<Dataset<Row>, Dataset<Row>> converter, Dataset<Row> batch)  {
        int batchId = batch.rdd().id();
        if (batch.isEmpty()) {
            logger.info("Batch: {} - Skipping empty batch", batchId);
        } else {
            logger.info("Batch: {} - Processing records", batchId);
            val startTime = System.currentTimeMillis();

            val dataFrame = converter.convert(batch);

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
            });

            logger.debug("Batch: {} - Processed records - processed batch in {}ms",
                    batchId,
                    System.currentTimeMillis() - startTime
            );
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
        return (dataFrame == null) ? null
                : dataFrame
                .filter(col(SOURCE).equalTo(source).and(col(TABLE).equalTo(table)))
                .orderBy(col(TIMESTAMP));
    }
}
