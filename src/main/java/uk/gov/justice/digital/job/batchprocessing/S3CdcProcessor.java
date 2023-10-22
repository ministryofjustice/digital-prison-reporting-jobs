package uk.gov.justice.digital.job.batchprocessing;

import jakarta.inject.Inject;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.service.DomainService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.zone.curated.CuratedZoneCDC;
import uk.gov.justice.digital.zone.structured.StructuredZoneCdcS3;

import javax.inject.Singleton;
import java.util.List;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.*;

/**
 * Responsible for processing batches of DMS records.
 * Dependencies which rely on the SparkSession are manually injected to ensure spark contexts/sessions/etc.
 * are created just once with a common configuration.
 */
@Singleton
public class S3CdcProcessor {

    private static final Logger logger = LoggerFactory.getLogger(S3CdcProcessor.class);

    private final StructuredZoneCdcS3 structuredZoneCdc;
    private final CuratedZoneCDC curatedZoneCdc;
    private final DomainService domainService;
    private final SourceReferenceService sourceReferenceService;
    private final ViolationService violationService;

    @Inject
    public S3CdcProcessor(
            StructuredZoneCdcS3 structuredZoneCdc,
            CuratedZoneCDC curatedZoneCdc,
            DomainService domainService,
            SourceReferenceService sourceReferenceService,
            ViolationService violationService
    ) {
        logger.info("Initializing S3BatchProcessor");
        this.structuredZoneCdc = structuredZoneCdc;
        this.curatedZoneCdc = curatedZoneCdc;
        this.sourceReferenceService = sourceReferenceService;
        this.domainService = domainService;
        this.violationService = violationService;
        logger.info("S3BatchProcessor initialization complete");
    }

    public void processCDC(SparkSession spark, Dataset<Row> dataFrame) {
        logger.info("Processing CDC batch");
        val cdcBatchStartTime = System.currentTimeMillis();

        getTablesInBatch(dataFrame).forEach(tableInfo -> {
            String sourceName = tableInfo.getAs(SOURCE);
            String tableName = tableInfo.getAs(TABLE);

            logger.info("Processing records {}/{}", sourceName, tableName);
            val startTime = System.currentTimeMillis();

            try {
                dataFrame.persist();
                val optionalSourceReference = sourceReferenceService.getSourceReference(sourceName, tableName);

                if (optionalSourceReference.isPresent()) {
                    val sourceReference = optionalSourceReference.get();
                    val structuredIncrementalDataFrame = structuredZoneCdc.process(spark, dataFrame, sourceReference);

                    dataFrame.unpersist();

                    val curatedCdcDataFrame = curatedZoneCdc.process(spark, structuredIncrementalDataFrame, sourceReference);

                    if (!curatedCdcDataFrame.isEmpty()) domainService
                            .refreshDomainUsingDataFrame(
                                    spark,
                                    curatedCdcDataFrame,
                                    sourceReference.getSource(),
                                    sourceReference.getTable()
                            );
                } else {
                    violationService.handleNoSchemaFound(spark, dataFrame, sourceName, tableName);
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

        logger.info("Processed CDC batch in {}ms", System.currentTimeMillis() - cdcBatchStartTime);
    }

    private List<Row> getTablesInBatch(Dataset<Row> dataFrame) {
        return dataFrame
                .select(TABLE, SOURCE, OPERATION)
                .dropDuplicates(TABLE, SOURCE)
                .collectAsList();
    }

}
