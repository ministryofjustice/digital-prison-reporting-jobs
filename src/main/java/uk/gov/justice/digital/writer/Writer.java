package uk.gov.justice.digital.writer;

import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;

import java.util.Arrays;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.TIMESTAMP;

public abstract class Writer {

    private static final Logger logger = LoggerFactory.getLogger(Writer.class);

    public abstract Dataset<Row> writeValidRecords(
            SparkSession spark,
            String destinationPath,
            SourceReference.PrimaryKey primaryKey,
            Dataset<Row> validRecords
    ) throws DataStorageException;

    public abstract void writeInvalidRecords(
            SparkSession spark,
            String destinationPath,
            Dataset<Row> invalidRecords
    ) throws DataStorageException;

    // static common methods
    protected static void appendDistinctRecords(
            SparkSession spark,
            DataStorageService storage,
            String destinationPath,
            SourceReference.PrimaryKey primaryKey,
            Dataset<Row> validRecords
    ) throws DataStorageException {
        logger.info("Appending {} records to deltalake table: {}", validRecords.count(), destinationPath);
        storage.appendDistinct(destinationPath, validRecords.drop(OPERATION), primaryKey);

        logger.info("Append completed successfully to table: {}", destinationPath);
        storage.updateDeltaManifestForTable(spark, destinationPath);
    }


    protected static void writeCdcRecords(
            SparkSession spark,
            DataStorageService storage,
            String destinationPath,
            SourceReference.PrimaryKey primaryKey,
            Dataset<Row> validRecords
    ) throws DataStorageRetriesExhaustedException {
        logger.info("Applying {} CDC records to deltalake table: {}", validRecords.count(), destinationPath);

        storage.mergeRecords(spark, destinationPath, validRecords, primaryKey, Arrays.asList(OPERATION, TIMESTAMP));

        logger.info("CDC records successfully applied to table: {}", destinationPath);
        storage.updateDeltaManifestForTable(spark, destinationPath);
    }

}
