package uk.gov.justice.digital.writer;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;

import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.Operation.*;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.TIMESTAMP;

public abstract class Writer {

    private static final Logger logger = LoggerFactory.getLogger(Writer.class);

    public abstract void writeValidRecords(
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
        storage.appendDistinct(destinationPath, validRecords.drop(OPERATION, TIMESTAMP), primaryKey);

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

        val inserts = validRecords.filter(col(OPERATION).equalTo(Insert.getName())).orderBy(TIMESTAMP);
        val updates = validRecords.filter(col(OPERATION).equalTo(Update.getName())).orderBy(TIMESTAMP);
        val deletes = validRecords.filter(col(OPERATION).equalTo(Delete.getName())).orderBy(TIMESTAMP);

        storage.upsertRecords(spark, destinationPath, inserts.drop(OPERATION, TIMESTAMP), primaryKey);
        storage.updateRecords(spark, destinationPath, updates.drop(OPERATION, TIMESTAMP), primaryKey);
        storage.deleteRecords(spark, destinationPath, deletes.drop(OPERATION, TIMESTAMP), primaryKey);

        logger.info("CDC records successfully applied to table: {}", destinationPath);
        storage.updateDeltaManifestForTable(spark, destinationPath);
    }

}
