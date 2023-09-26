package uk.gov.justice.digital.job;

import jakarta.inject.Inject;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.converter.dms.DMS_3_4_6;
import uk.gov.justice.digital.service.DomainService;
import uk.gov.justice.digital.zone.curated.CuratedZoneCDC;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoad;
import uk.gov.justice.digital.zone.raw.RawZone;
import uk.gov.justice.digital.zone.structured.StructuredZoneCDC;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoad;

import javax.inject.Singleton;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.SOURCE;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.TABLE;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.TIMESTAMP;

/**
 * Responsible for providing a BatchProcessor.
 * Dependencies which rely on the SparkSession are manually injected to ensure spark contexts/sessions/etc.
 * are created just once with a common configuration.
 */
@Singleton
public class BatchProcessorProvider {

    private static final Logger logger = LoggerFactory.getLogger(BatchProcessorProvider.class);

    private final RawZone rawZone;
    private final StructuredZoneLoad structuredZoneLoad;
    private final StructuredZoneCDC structuredZoneCDC;
    private final CuratedZoneLoad curatedZoneLoad;
    private final CuratedZoneCDC curatedZoneCDC;
    private final DomainService domainService;
    @Inject
    public BatchProcessorProvider(
            RawZone rawZone,
            StructuredZoneLoad structuredZoneLoad,
            StructuredZoneCDC structuredZoneCDC,
            CuratedZoneLoad curatedZoneLoad,
            CuratedZoneCDC curatedZoneCDC,
            DomainService domainService
    ) {
        this.rawZone = rawZone;
        this.structuredZoneLoad = structuredZoneLoad;
        this.structuredZoneCDC = structuredZoneCDC;
        this.curatedZoneLoad = curatedZoneLoad;
        this.curatedZoneCDC = curatedZoneCDC;
        this.domainService = domainService;
    }

    public BatchProcessor createBatchProcessor(SparkSession spark, DMS_3_4_6 converter) {
        return batch -> {
            int batchId = batch.rdd().id();
            if (batch.isEmpty()) {
                logger.info("Batch: {} - Skipping empty batch", batchId);
            } else {
                logger.info("Batch: {} - Processing records", batchId);
                val startTime = System.currentTimeMillis();

                val dataFrame = converter.convert(batch);

                getTablesInBatch(dataFrame).forEach(tableInfo -> {

                    try {
                        throw new Exception("Testing failure status!");
//                        val dataFrameForTable = extractDataFrameForSourceTable(dataFrame, tableInfo);
//                        dataFrameForTable.persist();
//
//                        rawZone.process(spark, dataFrameForTable, tableInfo);
//
//                        val structuredLoadDataFrame = structuredZoneLoad.process(spark, dataFrameForTable, tableInfo);
//                        val structuredIncrementalDataFrame = structuredZoneCDC.process(spark, dataFrameForTable, tableInfo);
//
//                        dataFrameForTable.unpersist();
//
//                        curatedZoneLoad.process(spark, structuredLoadDataFrame, tableInfo);
//                        val curatedCdcDataFrame = curatedZoneCDC.process(spark, structuredIncrementalDataFrame, tableInfo);
//
//                        if (!curatedCdcDataFrame.isEmpty()) domainService
//                                .refreshDomainUsingDataFrame(spark, curatedCdcDataFrame, tableInfo);

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
        };
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
