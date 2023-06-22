package uk.gov.justice.digital.zone;

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

import javax.inject.Inject;

import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.Operation.Load;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.OPERATION;

public class StructuredZoneLoad extends StructuredZone {

    private static final Logger logger = LoggerFactory.getLogger(StructuredZone.class);

    @Inject
    public StructuredZoneLoad(
            JobArguments jobArguments,
            DataStorageService storage,
            SourceReferenceService sourceReference
    ) {
        super(jobArguments, storage, sourceReference);
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, Row table) throws DataStorageException {
        val filteredRecords = dataFrame.filter(col(OPERATION).equalTo(Load.getName()));
        return super.process(spark, filteredRecords, table);
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

    @Override
    protected void writeInvalidRecords(
            SparkSession spark,
            DataStorageService storage,
            String tablePath,
            Dataset<Row> invalidRecords
    ) throws DataStorageException {
        logger.info("Appending {} records to deltalake table: {}", invalidRecords.count(), tablePath);
        storage.append(tablePath, invalidRecords.drop(OPERATION));
        logger.info("Append completed successfully");
        storage.updateDeltaManifestForTable(spark, tablePath);
    }

}
