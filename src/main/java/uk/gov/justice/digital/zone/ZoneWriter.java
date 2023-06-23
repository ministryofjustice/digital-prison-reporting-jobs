package uk.gov.justice.digital.zone;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;

public abstract class ZoneWriter {

    protected abstract void writeValidRecords(
            SparkSession spark,
            DataStorageService storage,
            String destinationPath,
            SourceReference.PrimaryKey primaryKey,
            Dataset<Row> validRecords
    ) throws DataStorageException;

    protected abstract void writeInvalidRecords(
            SparkSession spark,
            DataStorageService storage,
            String destinationPath,
            Dataset<Row> invalidRecords
    ) throws DataStorageException;

}
