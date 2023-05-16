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

import static uk.gov.justice.digital.job.model.Columns.SOURCE;
import static uk.gov.justice.digital.job.model.Columns.TABLE;

@Singleton
public class CuratedZone extends Zone {

    private static final Logger logger = LoggerFactory.getLogger(CuratedZone.class);

    private final String curatedPath;
    private final DataStorageService storage;

    @Inject
    public CuratedZone(JobArguments jobArguments, DataStorageService storage) {
        this.curatedPath = jobArguments.getCuratedS3Path();
        this.storage = storage;
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, Row table) throws DataStorageException {

        val count = dataFrame.count();

        logger.info("Processing batch with {} records", count);

        if (count > 0) {
            val startTime = System.currentTimeMillis();

            String sourceName = table.getAs(SOURCE);
            String tableName = table.getAs(TABLE);

            val sourceReference = SourceReferenceService
                    .getSourceReference(sourceName, tableName)
                    // This can only happen if the schema disappears after the structured zone has processed the data, so we
                    // should never see this in practise. However, if it does happen throwing here will make it clear what
                    // has happened.
                    .orElseThrow(() -> new IllegalStateException(
                            "Unable to locate source reference data for source: " + sourceName + " table: " + tableName
                    ));

            val curatedTablePath = this.storage.getTablePath(curatedPath, sourceReference);
            logger.info("Appending {} records to deltalake table: {}", dataFrame.count(), curatedTablePath);
            this.storage.append(curatedTablePath, dataFrame);
            logger.info("Append completed successfully");
            this.storage.updateDeltaManifestForTable(spark, curatedTablePath);

            logger.info("Processed dataframe with {} rows in {}ms",
                    count,
                    System.currentTimeMillis() - startTime
            );

            return dataFrame;
        } else return createEmptyDataFrame(dataFrame);
    }
}
