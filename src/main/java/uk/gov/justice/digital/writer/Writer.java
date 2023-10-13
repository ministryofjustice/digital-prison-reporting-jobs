package uk.gov.justice.digital.writer;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.converter.dms.DMS_3_4_7;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;

import java.util.ArrayList;
import java.util.Collections;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.Operation.getOperation;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;

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

        for (Row row: validRecords.collectAsList()) {
            processRow(spark, storage, destinationPath, primaryKey, row);
        }

        logger.info("CDC records successfully applied to table: {}", destinationPath);
        storage.updateDeltaManifestForTable(spark, destinationPath);
    }

    @NotNull
    protected static void processRow(
            SparkSession spark,
            DataStorageService storage,
            String destinationPath,
            SourceReference.PrimaryKey primaryKey,
            Row row
    ) throws DataStorageRetriesExhaustedException {
            String unvalidatedOperation = row.getAs(OPERATION);
            val optionalOperation = getOperation(unvalidatedOperation);

            if (optionalOperation.isPresent()) {
                val operation = optionalOperation.get();
                try {
                    writeRow(spark, storage, destinationPath, primaryKey, operation, row);
                } catch (DataStorageRetriesExhaustedException ex) {
                    // We rethrow because we do not want to catch this specific DataStorageException here
                    throw ex;
                } catch (DataStorageException ex) {
                    logger.error("Failed to {}", String.format("%s: %s to %s", operation.getName(), row.json(), destinationPath), ex);
                }
            } else {
                logger.error("Operation {} is invalid for {} to {}", unvalidatedOperation, row.json(), destinationPath);
            }
    }

    private static void writeRow(
            SparkSession spark,
            DataStorageService storage,
            String destinationPath,
            SourceReference.PrimaryKey primaryKey,
            DMS_3_4_7.Operation operation,
            Row row
    ) throws DataStorageException {
        val list = new ArrayList<>(Collections.singletonList(row));
        val dataFrame = spark.createDataFrame(list, row.schema()).drop(OPERATION);

        switch (operation) {
            case Insert:
                storage.upsertRecords(spark, destinationPath, dataFrame, primaryKey);
                break;
            case Update:
                storage.updateRecords(spark, destinationPath, dataFrame, primaryKey);
                break;
            case Delete:
                storage.deleteRecords(spark, destinationPath, dataFrame, primaryKey);
                break;
            default:
                logger.error(
                        "Operation {} is not allowed for incremental processing: {} to {}",
                        operation.getName(),
                        row.json(),
                        destinationPath
                );
                break;
        }
    }

}
