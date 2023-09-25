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
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.zone.Zone;

import javax.inject.Inject;
import javax.inject.Singleton;

import static org.apache.spark.sql.functions.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.*;

@Singleton
public class RawZone implements Zone {

    public static final String PRIMARY_KEY_NAME = "id";
    private static final Logger logger = LoggerFactory.getLogger(RawZone.class);

    private final String rawS3Path;
    private final DataStorageService storage;
    private final SourceReferenceService sourceReferenceService;

    @Inject
    public RawZone(JobArguments jobArguments,
                   DataStorageService storage,
                   SourceReferenceService sourceReferenceService) {
        this.rawS3Path = jobArguments.getRawS3Path();
        this.storage = storage;
        this.sourceReferenceService = sourceReferenceService;
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> records, Row table) throws DataStorageException {
        val startTime = System.currentTimeMillis();

        String rowSource = table.getAs(SOURCE);
        String rowTable = table.getAs(TABLE);

        logger.info("Processing batch for source: {} table: {}", rowSource, rowTable);

        val tablePath = getTablePath(rowSource, rowTable);

        logger.info("Applying batch to deltalake table: {}", tablePath);
        val rawDataFrame = createRawDataFrame(records);
        storage.appendDistinct(tablePath, rawDataFrame, new SourceReference.PrimaryKey(PRIMARY_KEY_NAME));

        logger.info("Append completed successfully");

        storage.updateDeltaManifestForTable(spark, tablePath);

        logger.info("Processed batch for {}/{} in {}ms", rowSource, rowTable, System.currentTimeMillis() - startTime);

        return rawDataFrame;
    }

    private static Dataset<Row> createRawDataFrame(Dataset<Row> dataset) {
        // this is the format that raw takes
        return dataset.select(
                concat(col(KEY), lit(":"), col(TIMESTAMP), lit(":"), col(OPERATION)).as(PRIMARY_KEY_NAME),
                col(TIMESTAMP), col(KEY), col(SOURCE), col(TABLE), col(OPERATION), col(CONVERTER), col(RAW));
    }

    private String getTablePath(String rowSource, String rowTable) {
        return sourceReferenceService.getSourceReference(rowSource, rowTable)
                .map(r -> createValidatedPath(rawS3Path, r.getSource(), r.getTable()))
                // Revert to source and table from row where no match exists in the schema reference service.
                .orElse(createValidatedPath(rawS3Path, rowSource, rowTable));
    }

}
