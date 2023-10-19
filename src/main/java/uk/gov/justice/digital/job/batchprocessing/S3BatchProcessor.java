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
import uk.gov.justice.digital.zone.curated.CuratedZoneLoad;
import uk.gov.justice.digital.zone.raw.RawZone;
import uk.gov.justice.digital.zone.structured.StructuredZoneCDC;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoad;

import javax.inject.Singleton;

/**
 * Responsible for processing batches of DMS records.
 * Dependencies which rely on the SparkSession are manually injected to ensure spark contexts/sessions/etc.
 * are created just once with a common configuration.
 */
@Singleton
public class S3BatchProcessor {

    private static final Logger logger = LoggerFactory.getLogger(S3BatchProcessor.class);

    private final RawZone rawZone;
    private final StructuredZoneLoad structuredZoneLoad;
    private final StructuredZoneCDC structuredZoneCDC;
    private final CuratedZoneLoad curatedZoneLoad;
    private final CuratedZoneCDC curatedZoneCDC;
    private final DomainService domainService;
    private final SourceReferenceService sourceReferenceService;
    private final ViolationService violationService;

    @Inject
    public S3BatchProcessor(
            RawZone rawZone,
            StructuredZoneLoad structuredZoneLoad,
            StructuredZoneCDC structuredZoneCDC,
            CuratedZoneLoad curatedZoneLoad,
            CuratedZoneCDC curatedZoneCDC,
            DomainService domainService,
            SourceReferenceService sourceReferenceService,
            ViolationService violationService
    ) {
        logger.info("Initializing S3BatchProcessor");
        this.rawZone = rawZone;
        this.structuredZoneLoad = structuredZoneLoad;
        this.structuredZoneCDC = structuredZoneCDC;
        this.curatedZoneLoad = curatedZoneLoad;
        this.curatedZoneCDC = curatedZoneCDC;
        this.domainService = domainService;
        this.sourceReferenceService = sourceReferenceService;
        this.violationService = violationService;
        logger.info("S3BatchProcessor initialization complete");
    }

    public void processBatch(SparkSession spark, String sourceName, String tableName, Dataset<Row> dataFrame) {
        try {
            dataFrame.persist();
            val optionalSourceReference = sourceReferenceService.getSourceReference(sourceName, tableName);

            if (optionalSourceReference.isPresent()) {
                val sourceReference = optionalSourceReference.get();

                rawZone.process(spark, dataFrame, sourceReference);

                val structuredLoadDataFrame = structuredZoneLoad.process(spark, dataFrame, sourceReference);
                val structuredIncrementalDataFrame = structuredZoneCDC.process(spark, dataFrame, sourceReference);

                dataFrame.unpersist();

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
                violationService.handleNoSchemaFound(spark, dataFrame, sourceName, tableName);
            }
        } catch (Exception e) {
            logger.error("Caught unexpected exception", e);
            throw new RuntimeException("Caught unexpected exception", e);
        }
    }

}
