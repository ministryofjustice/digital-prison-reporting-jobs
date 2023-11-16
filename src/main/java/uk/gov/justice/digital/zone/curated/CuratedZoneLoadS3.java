package uk.gov.justice.digital.zone.curated;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ViolationService;

import javax.inject.Inject;
import javax.inject.Singleton;

import static uk.gov.justice.digital.common.ResourcePath.tablePath;

@Singleton
public class CuratedZoneLoadS3 {

    private static final Logger logger = LoggerFactory.getLogger(CuratedZoneLoadS3.class);

    private final String curatedZoneRootPath;
    private final DataStorageService storage;
    private final ViolationService violationService;

    @Inject
    public CuratedZoneLoadS3(
            JobArguments arguments,
            DataStorageService storage,
            ViolationService violationService) {
        this.curatedZoneRootPath = arguments.getCuratedS3Path();
        this.storage = storage;
        this.violationService = violationService;
    }

    public void process(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference) throws DataStorageException {
        val startTime = System.currentTimeMillis();
        String sourceName = sourceReference.getSource();
        String tableName = sourceReference.getTable();
        SourceReference.PrimaryKey primaryKey = sourceReference.getPrimaryKey();
        val path = tablePath(curatedZoneRootPath, sourceName, tableName);
        logger.debug("Processing records for curated {}/{} {}", sourceName, tableName, path);
        try {
            logger.info("Appending {} records to deltalake table: {}", dataFrame.count(), path);
            storage.appendDistinct(path, dataFrame, primaryKey);
            logger.info("Append completed successfully to table: {}", path);
            storage.updateDeltaManifestForTable(spark, path);
            logger.info("Processed batch for curated {}/{} in {}ms", sourceName, tableName, System.currentTimeMillis() - startTime);
        } catch (DataStorageRetriesExhaustedException e) {
            logger.warn("Curated zone load retries exhausted", e);
            violationService.handleRetriesExhaustedS3(spark, dataFrame, sourceName, tableName, e, ViolationService.ZoneName.CURATED_LOAD);
        }
    }
}
