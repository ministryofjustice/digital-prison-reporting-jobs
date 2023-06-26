package uk.gov.justice.digital.writer;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.converter.dms.DMS_3_4_6;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.function.Consumer;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.Operation.getOperation;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.zone.raw.RawZone.PRIMARY_KEY_NAME;

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

        logger.info("Append completed successfully");
        storage.updateDeltaManifestForTable(spark, destinationPath);
    }


    protected static void writeCdcRecords(
            SparkSession spark,
            DataStorageService storage,
            String destinationPath,
            SourceReference.PrimaryKey primaryKey,
            Dataset<Row> validRecords
    ) {
        logger.info("Applying {} CDC records to deltalake table: {}", validRecords.count(), destinationPath);
        validRecords.collectAsList().forEach(processRow(spark, storage, destinationPath, primaryKey));

        logger.info("CDC records successfully applied");
        storage.updateDeltaManifestForTable(spark, destinationPath);
    }

    @NotNull
    protected static Consumer<Row> processRow(
            SparkSession spark,
            DataStorageService storage,
            String destinationPath,
            SourceReference.PrimaryKey primaryKey
    ) {
        return row -> {
            val optionalOperation = getOperation(row.getAs(OPERATION));
            if (optionalOperation.isPresent()) {
                val operation = optionalOperation.get();
                try {
                    writeRow(spark, storage, destinationPath, primaryKey, operation, row);
                } catch (DataStorageException ex) {
                    logger.warn("Failed to {}: {} to {}", operation.getName(), row.json(), destinationPath);
                }
            } else {
                logger.error("Operation invalid for {}", row.json());
            }
        };
    }

    private static void writeRow(
            SparkSession spark,
            DataStorageService storage,
            String destinationPath,
            SourceReference.PrimaryKey primaryKey,
            DMS_3_4_6.Operation operation,
            Row row
    ) throws DataStorageException {
        val list = new ArrayList<>(Collections.singletonList(row));
        val dataFrame = spark.createDataFrame(list, row.schema()).drop(OPERATION);

        switch (operation) {
            case Insert:
                storage.appendDistinct(destinationPath, dataFrame, primaryKey);
                break;
            case Update:
                storage.updateRecords(destinationPath, dataFrame, primaryKey);
                break;
            case Delete:
                storage.deleteRecords(destinationPath, dataFrame, PRIMARY_KEY_NAME);
                break;
            default:
                logger.warn(
                        "Operation {} is not allowed for incremental processing: {} to {}",
                        operation.getName(),
                        row.json(),
                        destinationPath
                );
                break;
        }
    }

}
