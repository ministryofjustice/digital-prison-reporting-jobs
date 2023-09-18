package uk.gov.justice.digital.writer.curated;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.writer.Writer;

import javax.inject.Inject;

public class CuratedZoneCdcWriter extends Writer {

    private static final long serialVersionUID = -7393921506128460852L;
    private final DataStorageService storage;

    @Inject
    public CuratedZoneCdcWriter(DataStorageService storage) {
        this.storage = storage;
    }

    public void writeValidRecords(
            SparkSession spark,
            String destinationPath,
            SourceReference.PrimaryKey primaryKey,
            Dataset<Row> validRecords
    ) {
        writeCdcRecords(spark, storage, destinationPath, primaryKey, validRecords);
    }

    @Override
    public void writeInvalidRecords(
            SparkSession spark,
            String destinationPath,
            Dataset<Row> invalidRecords
    ) {}

}
