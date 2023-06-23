package uk.gov.justice.digital.zone;

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

import javax.inject.Inject;
import javax.inject.Singleton;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.OPERATION;

@Singleton
public class CuratedZoneLoad extends CuratedZone {

    private static final Logger logger = LoggerFactory.getLogger(CuratedZoneLoad.class);

    @Inject
    public CuratedZoneLoad(
            JobArguments jobArguments,
            DataStorageService storage,
            SourceReferenceService sourceReferenceService
    ) {
        super(jobArguments, storage, sourceReferenceService);
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, Row table) throws DataStorageException {
        return super.process(spark, dataFrame, table);
    }

    @Override
    protected void writeValidRecords(
            SparkSession spark,
            DataStorageService storage,
            String tablePath,
            SourceReference.PrimaryKey primaryKey,
            Dataset<Row> validRecords
    ) throws DataStorageException {
        logger.info("Appending {} records to deltalake table: {}", validRecords.count(), tablePath);
        storage.appendDistinct(tablePath, validRecords.drop(OPERATION), primaryKey);
        logger.info("Append completed successfully");
        storage.updateDeltaManifestForTable(spark, tablePath);
    }

}
