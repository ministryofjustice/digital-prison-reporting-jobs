package uk.gov.justice.digital.zone.curated;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ViolationService;

import static uk.gov.justice.digital.common.ResourcePath.tablePath;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.STRUCTURED_CDC;

@Singleton
public class CuratedZoneCDC {

    private static final Logger logger = LoggerFactory.getLogger(CuratedZoneCDC.class);

    private final String curatedZoneRootPath;
    private final ViolationService violationService;
    private final DataStorageService storage;

    @Inject
    public CuratedZoneCDC(
            JobArguments arguments,
            ViolationService violationService,
            DataStorageService storage) {
        this.curatedZoneRootPath = arguments.getCuratedS3Path();
        this.violationService = violationService;
        this.storage = storage;
    }


    public void process(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference) {
        val startTime = System.currentTimeMillis();
        String sourceName = sourceReference.getSource();
        String tableName = sourceReference.getTable();
        String curatedTablePath = tablePath(curatedZoneRootPath, sourceName, tableName);
        logger.debug("Processing records for curated {}/{} {}", sourceName, tableName, curatedTablePath);

        try {
            logger.debug("Merging {} records to deltalake table: {}", dataFrame.count(), curatedTablePath);
            storage.mergeRecordsCdc(spark, curatedTablePath, dataFrame, sourceReference.getPrimaryKey());
            logger.debug("Merge completed successfully to table: {}", curatedTablePath);
            storage.updateDeltaManifestForTable(spark, curatedTablePath);
            logger.info("Processed batch for curated {}/{} in {}ms", sourceName, tableName, System.currentTimeMillis() - startTime);
        } catch (DataStorageRetriesExhaustedException e) {
            logger.warn("Curated zone cdc retries exhausted", e);
            violationService.handleRetriesExhausted(spark, dataFrame, sourceName, tableName, e, STRUCTURED_CDC);
        }
    }


}
