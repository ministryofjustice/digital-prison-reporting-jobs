package uk.gov.justice.digital.writer.curated;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.writer.Writer;

import javax.inject.Inject;

public class CuratedZoneLoadWriter extends Writer {

    private final DataStorageService storage;

    @Inject
    public CuratedZoneLoadWriter(DataStorageService storage) {
        this.storage = storage;
    }

    @Override
    public void writeValidRecords(
            SparkSession spark,
            String destinationPath,
            SourceReference.PrimaryKey primaryKey,
            Dataset<Row> validRecords
    ) throws DataStorageException {
        appendDistinctRecords(spark, storage, destinationPath, primaryKey, validRecords);
    }

    @Override
    public void writeInvalidRecords(
            SparkSession spark,
            String destinationPath,
            Dataset<Row> invalidRecords
    ) {}

}
