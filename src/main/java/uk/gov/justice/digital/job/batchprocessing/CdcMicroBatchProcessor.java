package uk.gov.justice.digital.job.batchprocessing;

import jakarta.inject.Inject;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.converter.dms.DMS_3_4_7;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.ViolationService;

import javax.inject.Singleton;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.Operation.Delete;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.Operation.Insert;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.Operation.Update;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.SOURCE;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.TABLE;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.TIMESTAMP;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ShortOperationCode.cdcShortOperationCodes;

/**
 * Responsible for processing batches of DMS records.
 * Dependencies which rely on the SparkSession are manually injected to ensure spark contexts/sessions/etc.
 * are created just once with a common configuration.
 */
@Singleton
public class CdcMicroBatchProcessor {

    private static final Logger logger = LoggerFactory.getLogger(CdcMicroBatchProcessor.class);

    private final SourceReferenceService sourceReferenceService;
    private final ViolationService violationService;
    private final PerTableBatchCdcProcessor perTableProcessor;

    @Inject
    public CdcMicroBatchProcessor(
            SourceReferenceService sourceReferenceService,
            ViolationService violationService,
            PerTableBatchCdcProcessor perTableProcessor) {
        logger.info("Initializing S3BatchProcessor");
        this.sourceReferenceService = sourceReferenceService;
        this.violationService = violationService;
        this.perTableProcessor = perTableProcessor;
        logger.info("S3BatchProcessor initialization complete");
    }

    public void processCDC(SparkSession spark, Dataset<Row> batch) {
        logger.info("Processing CDC batch");
        val cdcBatchStartTime = System.currentTimeMillis();
        val dataFrame = convert(batch);
        dataFrame.persist();

        getTablesInBatch(dataFrame).forEach(tableInfo -> {
            val startTime = System.currentTimeMillis();
            String sourceName = tableInfo.getAs(SOURCE);
            String tableName = tableInfo.getAs(TABLE);
            logger.info("Processing records {}/{}", sourceName, tableName);
            try {
                val dataFrameForTable = extractDataFrameForSourceTable(dataFrame, tableInfo);
                val optionalSourceReference = sourceReferenceService.getSourceReference(sourceName, tableName);
                if (optionalSourceReference.isPresent()) {
                    perTableProcessor.processTable(spark, dataFrameForTable, optionalSourceReference.get());
                } else {
                    violationService.handleNoSchemaFound(spark, dataFrameForTable, sourceName, tableName);
                }
                logger.info("Processed records {}/{} in {}ms",
                        sourceName,
                        tableName,
                        System.currentTimeMillis() - startTime
                );
            } catch (Exception e) {
                logger.error("Caught unexpected exception", e);
                throw new RuntimeException("Caught unexpected exception", e);
            }
        });
        dataFrame.unpersist();
        logger.info("Processed CDC batch in {}ms", System.currentTimeMillis() - cdcBatchStartTime);
    }

    private static Dataset<Row> convert(Dataset<Row> batch) {
        val shortOperationColumnName = "Op";
        return batch
                .filter(col(shortOperationColumnName).isin(cdcShortOperationCodes))
                .withColumn(
                        OPERATION,
                        when(col(shortOperationColumnName).equalTo(lit(DMS_3_4_7.ShortOperationCode.Insert.getName())), lit(Insert.getName()))
                                .when(col(shortOperationColumnName).equalTo(lit(DMS_3_4_7.ShortOperationCode.Update.getName())), lit(Update.getName()))
                                .when(col(shortOperationColumnName).equalTo(lit(DMS_3_4_7.ShortOperationCode.Delete.getName())), lit(Delete.getName()))
                );
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
