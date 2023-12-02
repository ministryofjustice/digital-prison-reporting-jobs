package uk.gov.justice.digital.zone.structured;

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
public class StructuredZoneLoadS3 {

    private static final Logger logger = LoggerFactory.getLogger(StructuredZoneLoadS3.class);
    private final String structuredZoneRootPath;
    private final DataStorageService storage;
    private final ViolationService violationService;

    @Inject
    public StructuredZoneLoadS3(
            JobArguments arguments,
            DataStorageService storage,
            ViolationService violationService) {
        this.structuredZoneRootPath = arguments.getStructuredS3Path();
        this.storage = storage;
        this.violationService = violationService;
    }

    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference) throws DataStorageException {
        val startTime = System.currentTimeMillis();
        String sourceName = sourceReference.getSource();
        String tableName = sourceReference.getTable();
        SourceReference.PrimaryKey primaryKey = sourceReference.getPrimaryKey();
        val path = tablePath(structuredZoneRootPath, sourceName, tableName);
        logger.debug("Processing records for structured {}/{} {}", sourceName, tableName, path);
        Dataset<Row> result = dataFrame;
        try {
            logger.info("Appending {} records to deltalake table: {}", dataFrame.count(), path);
            storage.appendDistinct(path, dataFrame, primaryKey);
            logger.info("Append completed successfully to table: {}", path);
            storage.updateDeltaManifestForTable(spark, path);
            logger.info("Processed batch for structured {}/{} in {}ms", sourceName, tableName, System.currentTimeMillis() - startTime);
        } catch (DataStorageRetriesExhaustedException e) {
            logger.warn("Structured zone load retries exhausted", e);
            violationService.handleRetriesExhaustedS3(spark, dataFrame, sourceName, tableName, e, ViolationService.ZoneName.STRUCTURED_LOAD);
            result = spark.emptyDataFrame();
        }
        return result;
    }
}
