package uk.gov.justice.digital.zone;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.converter.dms.DMS_3_4_6;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.function.Consumer;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.Operation.getOperation;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.zone.RawZone.PRIMARY_KEY_NAME;

@Singleton
public class CuratedZoneCDC extends CuratedZone {

    private static final Logger logger = LoggerFactory.getLogger(CuratedZoneCDC.class);

    @Inject
    public CuratedZoneCDC(
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
    ) {
        logger.info("Applying {} CDC records to deltalake table: {}", validRecords.count(), tablePath);
        validRecords.collectAsList().forEach(processRow(spark, storage, tablePath, primaryKey));

        logger.info("CDC records successfully applied");
        storage.updateDeltaManifestForTable(spark, tablePath);
    }

    @NotNull
    private Consumer<Row> processRow(
            SparkSession spark,
            DataStorageService storage,
            String tablePath,
            SourceReference.PrimaryKey primaryKey
    ) {
        return row -> {
            val optionalOperation = getOperation(row.getAs(OPERATION));
            if (optionalOperation.isPresent()) {
                val operation = optionalOperation.get();
                try {
                    writeRow(spark, storage, tablePath, primaryKey, operation, row);
                } catch (DataStorageException ex) {
                    logger.warn("Failed to {}: {} to {}", operation.getName(), row.json(), tablePath);
                }
            } else {
                logger.error("Operation invalid for {}", row.json());
            }
        };
    }

    private void writeRow(
            SparkSession spark,
            DataStorageService storage,
            String tablePath,
            SourceReference.PrimaryKey primaryKey,
            DMS_3_4_6.Operation operation,
            Row row
    ) throws DataStorageException {
        val list = new ArrayList<>(Collections.singletonList(row));
        val dataFrame = spark.createDataFrame(list, row.schema()).drop(OPERATION);

        switch (operation) {
            case Insert:
                storage.appendDistinct(tablePath, dataFrame, primaryKey);
                break;
            case Update:
                storage.updateRecords(tablePath, dataFrame, primaryKey);
                break;
            case Delete:
                storage.deleteRecords(tablePath, dataFrame, PRIMARY_KEY_NAME);
                break;
            default:
                logger.warn(
                        "Operation {} is not allowed for incremental processing: {} to {}",
                        operation.getName(),
                        row.json(),
                        tablePath
                );
                break;
        }
    }

}
