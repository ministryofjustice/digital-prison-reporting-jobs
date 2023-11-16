package uk.gov.justice.digital.zone;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoadS3;

import static java.lang.String.format;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;

public class ZoneLoad {

    private static final Logger logger = LoggerFactory.getLogger(CuratedZoneLoadS3.class);

    private final DataStorageService storage;
    private final ViolationService violationService;
    private final String zoneRootPath;
    private final ViolationService.ZoneName zoneName;

    public ZoneLoad(
            DataStorageService storage,
            ViolationService violationService,
            String zoneRootPath,
            ViolationService.ZoneName zoneName) {
        this.storage = storage;
        this.violationService = violationService;
        this.zoneRootPath = zoneRootPath;
        this.zoneName = zoneName;
    }

    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference) throws DataStorageException {
        Dataset<Row> result;
        try {
            val startTime = System.currentTimeMillis();

            String sourceName = sourceReference.getSource();
            String tableName = sourceReference.getTable();

            logger.debug("Processing records for {} {}/{}", zoneName.toString(), sourceName, tableName);
            val tablePath = createValidatedPath(zoneRootPath, sourceReference.getSource(), sourceReference.getTable());

            storage.appendDistinctRecords(spark, dataFrame, tablePath, sourceReference.getPrimaryKey());
            result = dataFrame;
            logger.debug("Processed batch for {} {}/{} in {}ms", zoneName, sourceName, tableName, System.currentTimeMillis() - startTime);
        } catch (DataStorageRetriesExhaustedException e) {
            logger.warn(format("%s zone load retries exhausted", zoneName), e);
            violationService.handleRetriesExhaustedS3(spark, dataFrame, sourceReference.getSource(), sourceReference.getTable(), e, zoneName);
            result = spark.emptyDataFrame();
        }
        return result;
    }
}
