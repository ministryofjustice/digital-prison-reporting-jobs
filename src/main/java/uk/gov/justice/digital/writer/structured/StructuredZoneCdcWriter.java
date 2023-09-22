package uk.gov.justice.digital.writer.structured;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.writer.Writer;

import javax.inject.Inject;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;

public class StructuredZoneCdcWriter extends Writer {

    private static final Logger logger = LoggerFactory.getLogger(StructuredZoneCdcWriter.class);

    private final DataStorageService storage;

    @Inject
    public StructuredZoneCdcWriter(DataStorageService storage) {
        this.storage = storage;
    }

    @Override
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
    ) throws DataStorageException {
        logger.info("Appending {} records to deltalake table: {}", invalidRecords.count(), destinationPath);
        storage.append(destinationPath, invalidRecords.drop(OPERATION));

        logger.info("Append completed successfully");
        storage.updateDeltaManifestForTable(spark, destinationPath);
    }

}
