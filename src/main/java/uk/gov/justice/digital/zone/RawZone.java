package uk.gov.justice.digital.zone;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;

import javax.inject.Inject;
import javax.inject.Singleton;

import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.common.ColumnNames.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;

@Singleton
public class RawZone extends Zone {

    private static final Logger logger = LoggerFactory.getLogger(RawZone.class);

    private final String rawS3Path;
    private final DataStorageService storage;

    @Inject
    public RawZone(JobArguments jobArguments, DataStorageService storage) {
        this.rawS3Path = jobArguments.getRawS3Path();
        this.storage = storage;
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, Row table) throws DataStorageException {

        logger.info("Processing data frame with {} rows", dataFrame.count());

        val startTime = System.currentTimeMillis();

        String rowSource = table.getAs(SOURCE);
        String rowTable = table.getAs(TABLE);
        String rowOperation = table.getAs(OPERATION);
        // TODO - review table path construction here
        val tablePath = SourceReferenceService.getSourceReference(rowSource, rowTable)
                .map(r -> createValidatedPath(rawS3Path, r.getSource(), r.getTable(), rowOperation))
                // Revert to source and table from row where no match exists in the schema reference service.
                .orElse(createValidatedPath(rawS3Path, rowSource, rowTable, rowOperation));

        val rawDataFrame = extractRawDataFrame(dataFrame, rowSource, rowTable);

        logger.info("Appending {} records to deltalake table: {}", rawDataFrame.count(), tablePath);
        storage.append(tablePath, rawDataFrame);
        logger.info("Append completed successfully");
        storage.updateDeltaManifestForTable(spark, tablePath);

        logger.info("Processed data frame with {} rows in {}ms",
                rawDataFrame.count(),
                System.currentTimeMillis() - startTime
        );

        return rawDataFrame;
    }

    protected Dataset<Row> extractRawDataFrame(Dataset<Row> dataFrame, String rowSource, String rowTable) {
        return (dataFrame == null) ? null
                : dataFrame
                    .filter(col(SOURCE).equalTo(rowSource).and(col(TABLE).equalTo(rowTable)))
                    .drop(SOURCE, TABLE, OPERATION);
    }

}
