package uk.gov.justice.digital.job.batchprocessing;

import jakarta.inject.Inject;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoadS3;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoadS3;

import javax.inject.Singleton;

import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_LOAD;

/**
 * Responsible for processing batches of DMS records.
 * Dependencies which rely on the SparkSession are manually injected to ensure spark contexts/sessions/etc.
 * are created just once with a common configuration.
 */
@Singleton
public class S3BatchProcessor {

    private static final Logger logger = LoggerFactory.getLogger(S3BatchProcessor.class);

    private final StructuredZoneLoadS3 structuredZoneLoad;
    private final CuratedZoneLoadS3 curatedZoneLoad;
    private final ValidationService validationService;

    @Inject
    public S3BatchProcessor(
            StructuredZoneLoadS3 structuredZoneLoad,
            CuratedZoneLoadS3 curatedZoneLoad,
            ValidationService validationService) {
        this.validationService = validationService;
        logger.info("Initializing S3BatchProcessor");
        this.structuredZoneLoad = structuredZoneLoad;
        this.curatedZoneLoad = curatedZoneLoad;
        logger.info("S3BatchProcessor initialization complete");
    }

    public void processBatch(SparkSession spark, SourceReference sourceReference, Dataset<Row> dataFrame) {
        String sourceName = sourceReference.getSource();
        String tableName = sourceReference.getTable();
        logger.info("Processing records {}/{}", sourceName, tableName);

        val startTime = System.currentTimeMillis();
        dataFrame.persist();
        try {
            val filteredDf = dataFrame.where(col(OPERATION).equalTo(Insert.getName()));
            val validRows = validationService.handleValidation(spark, filteredDf, sourceReference, STRUCTURED_LOAD);
            val structuredLoadDf = structuredZoneLoad.process(spark, validRows, sourceReference);
            curatedZoneLoad.process(spark, structuredLoadDf, sourceReference);
        } catch (Exception e) {
            logger.error("Caught unexpected exception", e);
            throw new RuntimeException("Caught unexpected exception", e);
        }
        dataFrame.unpersist();

        logger.info("Processed records {}/{} in {}ms",
                sourceName,
                tableName,
                System.currentTimeMillis() - startTime
        );
    }
}
