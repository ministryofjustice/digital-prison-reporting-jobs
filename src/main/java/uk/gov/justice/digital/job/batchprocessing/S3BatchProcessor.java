package uk.gov.justice.digital.job.batchprocessing;

import jakarta.inject.Inject;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoad;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoadS3;

import javax.inject.Singleton;

/**
 * Responsible for processing batches of DMS records.
 * Dependencies which rely on the SparkSession are manually injected to ensure spark contexts/sessions/etc.
 * are created just once with a common configuration.
 */
@Singleton
public class S3BatchProcessor {

    private static final Logger logger = LoggerFactory.getLogger(S3BatchProcessor.class);

    private final StructuredZoneLoadS3 structuredZoneLoad;
    private final CuratedZoneLoad curatedZoneLoad;
    private final SourceReferenceService sourceReferenceService;
    private final ViolationService violationService;

    @Inject
    public S3BatchProcessor(
            StructuredZoneLoadS3 structuredZoneLoad,
            CuratedZoneLoad curatedZoneLoad,
            SourceReferenceService sourceReferenceService,
            ViolationService violationService
    ) {
        logger.info("Initializing S3BatchProcessor");
        this.structuredZoneLoad = structuredZoneLoad;
        this.curatedZoneLoad = curatedZoneLoad;
        this.sourceReferenceService = sourceReferenceService;
        this.violationService = violationService;
        logger.info("S3BatchProcessor initialization complete");
    }

    public void processBatch(SparkSession spark, String sourceName, String tableName, Dataset<Row> dataFrame) {
        logger.info("Processing records {}/{}", sourceName, tableName);

        val startTime = System.currentTimeMillis();
        dataFrame.persist();
        try {
            val optionalSourceReference = sourceReferenceService.getSourceReference(sourceName, tableName);

            if (optionalSourceReference.isPresent()) {
                val sourceReference = optionalSourceReference.get();
                val structuredLoadDataFrame = structuredZoneLoad.process(spark, dataFrame, sourceReference);
                curatedZoneLoad.process(spark, structuredLoadDataFrame, sourceReference);
            } else {
                violationService.handleNoSchemaFound(spark, dataFrame, sourceName, tableName);
            }
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
