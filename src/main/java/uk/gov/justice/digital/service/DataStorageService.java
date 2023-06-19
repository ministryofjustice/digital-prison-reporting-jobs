package uk.gov.justice.digital.service;

import io.delta.tables.DeltaTable;
import lombok.val;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.domain.model.TableIdentifier;
import uk.gov.justice.digital.exception.DataStorageException;

import javax.inject.Singleton;
import java.util.Optional;

@Singleton
public class DataStorageService {

    private static final String SOURCE = "source";
    private static final String TARGET = "target";

    private static final Logger logger = LoggerFactory.getLogger(DataStorageService.class);

    public boolean exists(SparkSession spark, TableIdentifier tableId) {
        val exists = DeltaTable.isDeltaTable(spark, tableId.toPath());
        logger.info("Delta table path {} {}", tableId.toPath(), (exists) ? "exists" : "does not exist");
        return exists;
    }


    public boolean hasRecords(SparkSession spark, TableIdentifier tableId) throws DataStorageException {
        val hasRecords = exists(spark, tableId) &&
                Optional.ofNullable(get(spark, tableId))
                        .map(df -> !df.isEmpty())
                        .orElse(false);

        logger.info("Delta table {} {}", tableId.getSchema(), (hasRecords) ? "contains data" : "is empty");

        return hasRecords;
    }

    public void append(String tablePath, Dataset<Row> df) throws DataStorageException {
        logger.info("Appending schema and data to " + tablePath);
        if (tablePath != null && df != null)
            df.write()
                    .format("delta")
                    .mode("append")
                    .option("path", tablePath)
                    .save();
        else {
            logger.error("Path is not set or dataframe is null");
            throw new DataStorageException("Path is not set or dataframe is null");
        }
    }

    public void appendDistinct(String tablePath, Dataset<Row> df, SourceReference.PrimaryKey primaryKey) throws DataStorageException {
        if(!df.isEmpty()) {
            final DeltaTable dt = getTable(df.sparkSession(), tablePath);
            if(dt != null) {
                final String condition = primaryKey.getSparkCondition(SOURCE, TARGET);
                dt.as(SOURCE)
                        .merge(df.as(TARGET), condition )
                        .whenNotMatched().insertAll()
                        .execute();
            } else {
                append(tablePath, df);
            }
        }
    }

    public void update(String tablePath, Dataset<Row> dataFrame, String primaryKey) throws DataStorageException {
        val dt = getTable(dataFrame.sparkSession(), tablePath);

        if (dt != null) {
            val spk = SOURCE + "." + primaryKey;
            val tpk = TARGET + "." + primaryKey;
            dt.as(SOURCE)
                    .merge(dataFrame.as(TARGET), spk + "=" + tpk)
                    .whenMatched()
                    .updateAll()
                    .execute();
        } else {
            val errorMessage = "Failed to access Delta table for update";
            logger.error(errorMessage);
            throw new DataStorageException(errorMessage);
        }
    }

    public void deleteRecords(String tablePath, Dataset<Row> dataFrame, String primaryKey) throws DataStorageException {
        val dt = getTable(dataFrame.sparkSession(), tablePath);

        if (dt != null) {
            dt.delete(functions.col(primaryKey).eqNullSafe(dataFrame.col(primaryKey)));
        } else {
            val errorMessage = "Failed to access Delta table for delete";
            logger.error(errorMessage);
            throw new DataStorageException(errorMessage);
        }
    }

    public void create(String tablePath, Dataset<Row> df) throws DataStorageException {
        logger.info("Inserting schema and data to " + tablePath);
        if (tablePath != null && df != null)
            df.write()
                    .format("delta")
                    .option("path", tablePath)
                    .save();
        else {
            logger.error("Path is not set or dataframe is null");
            throw new DataStorageException("Path is not set or dataframe is null");
        }
    }

    public void replace(String tablePath, Dataset<Row> df) throws DataStorageException {
        logger.info("Overwriting schema and data to " + tablePath);
        if (tablePath != null && df != null)
            df.write()
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", true)
                    .option("path", tablePath)
                    .save();
        else {
            logger.error("Path is not set or dataframe is null");
            throw new DataStorageException("Path is not set or dataframe is null");
        }
    }

    public void resync(String tablePath, Dataset<Row> df) throws DataStorageException {
        logger.info("Syncing data to " + tablePath);
        if (tablePath != null && df != null)
            df.write()
                    .format("delta")
                    .mode("overwrite")
                    .option("path", tablePath)
                    .save();
        else {
            logger.error("Path is not set or dataframe is null");
            throw new DataStorageException("Path is not set or dataframe is null");
        }

    }

    public void delete(SparkSession spark, TableIdentifier tableId) throws DataStorageException {
        logger.info("Deleting Delta table {}.{}", tableId.getSchema(), tableId.getTable());

        val deltaTable = Optional.ofNullable(getTable(spark, tableId.toPath()))
                .orElseThrow(() -> new DataStorageException("Failed to delete table. Table does not exist"));

        deltaTable.delete();
    }

    public void vacuum(SparkSession spark, TableIdentifier tableId) throws DataStorageException {
        logger.info("Vacuuming Delta table {}.{}", tableId.getSchema(), tableId.getTable());

        val deltaTable = Optional.ofNullable(getTable(spark, tableId.toPath()))
                .orElseThrow(() -> new DataStorageException("Failed to vacuum table. Table does not exist"));

        deltaTable.vacuum();
    }

    public Dataset<Row> get(SparkSession spark, TableIdentifier tableId) {
        return get(spark, tableId.toPath());
    }

    public Dataset<Row> get(SparkSession spark, String tablePath) {
        DeltaTable deltaTable = getTable(spark, tablePath);
        return deltaTable == null ? null : deltaTable.toDF();
    }

    protected DeltaTable getTable(SparkSession spark, String tablePath) {
        if (DeltaTable.isDeltaTable(spark, tablePath))
            return DeltaTable.forPath(spark, tablePath);
        else {
            logger.warn("Cannot update manifest for table: {} - Not a delta table", tablePath);
        }
        return null;
    }

    public void endTableUpdates(SparkSession spark, TableIdentifier tableId) throws DataStorageException {
        updateDeltaManifestForTable(spark, tableId.toPath());
    }

    protected void updateManifest(DeltaTable dt) {
        dt.generate("symlink_format_manifest");
    }

    public void updateDeltaManifestForTable(SparkSession spark, String tablePath) {
        DeltaTable deltaTable = getTable(spark, tablePath);
        updateManifest(deltaTable);
    }

}
