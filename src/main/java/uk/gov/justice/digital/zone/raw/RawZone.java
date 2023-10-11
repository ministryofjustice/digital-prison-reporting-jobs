package uk.gov.justice.digital.zone.raw;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.zone.Zone;

import javax.inject.Inject;
import javax.inject.Singleton;

import static org.apache.spark.sql.functions.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.*;

@Singleton
public class RawZone implements Zone {

    public static final String PRIMARY_KEY_NAME = "id";
    public static final SourceReference.PrimaryKey primaryKey = new SourceReference.PrimaryKey(PRIMARY_KEY_NAME);
    private static final Logger logger = LoggerFactory.getLogger(RawZone.class);

    private final String rawS3Path;
    private final DataStorageService storage;

    @Inject
    public RawZone(JobArguments jobArguments, DataStorageService storage) {
        this.rawS3Path = jobArguments.getRawS3Path();
        this.storage = storage;
    }

    @Override
    public Dataset<Row> process(
            SparkSession spark,
            Dataset<Row> records,
            SourceReference sourceReference
    ) throws DataStorageException {
        val startTime = System.currentTimeMillis();

        String rowSource = sourceReference.getSource();
        String rowTable = sourceReference.getTable();

        logger.debug("Processing batch for source: {} table: {}", rowSource, rowTable);

        val tablePath = getTablePath(sourceReference);

        logger.debug("Applying batch to deltalake table: {}", tablePath);
        val rawDataFrame = createRawDataFrame(records);
        storage.appendDistinct(tablePath, rawDataFrame, primaryKey);

        logger.debug("Append completed successfully");

        storage.updateDeltaManifestForTable(spark, tablePath);

        logger.debug("Processed batch for {}/{} in {}ms", rowSource, rowTable, System.currentTimeMillis() - startTime);

        return rawDataFrame;
    }

    private static Dataset<Row> createRawDataFrame(Dataset<Row> dataset) {
        // this is the format that raw takes
        return dataset.select(
                concat(col(KEY), lit(":"), col(TIMESTAMP), lit(":"), col(OPERATION)).as(PRIMARY_KEY_NAME),
                col(TIMESTAMP), col(KEY), col(SOURCE), col(TABLE), col(OPERATION), col(CONVERTER), col(RAW));
    }

    private String getTablePath(SourceReference sourceReference) {
        return createValidatedPath(rawS3Path, sourceReference.getSource(), sourceReference.getTable());
    }

}
