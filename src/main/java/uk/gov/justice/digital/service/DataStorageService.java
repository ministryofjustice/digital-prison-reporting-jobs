package uk.gov.justice.digital.service;

import io.delta.tables.DeltaTable;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domain.model.TableIdentifier;
import uk.gov.justice.digital.exception.DataStorageException;

import javax.inject.Singleton;

@Singleton
public class DataStorageService {

    private static final Logger logger = LoggerFactory.getLogger(DataStorageService.class);

    public boolean exists(final SparkSession spark, final TableIdentifier tableId) {
        val tablePath = tableId.toPath();
        logger.info("Delta table path {}", tablePath);
        return DeltaTable.isDeltaTable(spark, tablePath);
    }

    public boolean hasRecords(final SparkSession spark, final TableIdentifier info) throws DataStorageException {
        logger.info("Checking details for Delta table..." + info.getSchema() + "." + info.getTable());
        if(exists(spark, info)) {
            Dataset<Row> df = load(spark, info);
            return !df.isEmpty();
        } else {
            return false;
        }
    }

    public void append(final String tablePath, final Dataset<Row> df) throws DataStorageException {
        logger.info("Appending schema and data to " + tablePath);
        if (tablePath != null && df != null)
            df.write()
                    .format("delta")
                    .mode("append")
                    .option("path", tablePath)
                    .save();
        else throw new DataStorageException("Path is not set or dataframe is null");
    }

    public void create(final String tablePath, final Dataset<Row> df) throws DataStorageException {
        logger.info("Inserting schema and data to " + tablePath);
        if (tablePath != null && df != null)
            df.write()
                    .format("delta")
                    .option("path", tablePath)
                    .save();
        else throw new DataStorageException("Path is not set or dataframe is null");
    }

    public void replace(final String tablePath, final Dataset<Row> df) throws DataStorageException {
        logger.info("Overwriting schema and data to " + tablePath);
        if (tablePath != null && df != null)
            df.write()
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", true)
                    .option("path", tablePath)
                    .save();
        else throw new DataStorageException("Path is not set or dataframe is null");
    }

    public void reload(final String tablePath, final Dataset<Row> df) throws DataStorageException {
        logger.info("Syncing data to " + tablePath);
        if (tablePath != null && df != null)
            df.write()
                    .format("delta")
                    .mode("overwrite")
                    .option("path", tablePath)
                    .save();
        else throw new DataStorageException("Path is not set or dataframe is null");
    }

    public void delete(final SparkSession spark, final TableIdentifier tableId) throws DataStorageException {
        logger.info("deleting Delta table..." + tableId.getSchema() + "." + tableId.getTable());
        String tablePath = tableId.toPath();
        final DeltaTable deltaTable = getTable(spark, tablePath);
        if (deltaTable != null) {
            deltaTable.delete();
        } else throw new DataStorageException("Delta table delete failed");
    }

    public void vacuum(final SparkSession spark, final TableIdentifier tableId) throws DataStorageException {
        final DeltaTable deltaTable = getTable(spark, tableId.toPath());
        if (deltaTable != null) {
            deltaTable.vacuum();
        } else throw new DataStorageException("Delta table vaccum failed");
    }

    public Dataset<Row> load(final SparkSession spark, final TableIdentifier tableId) {
        String tablePath = tableId.toPath();
        final DeltaTable deltaTable = getTable(spark, tablePath);
        return deltaTable == null ? null : deltaTable.toDF();
    }

    protected DeltaTable getTable(final SparkSession spark, final String tablePath) {
        if (DeltaTable.isDeltaTable(spark, tablePath))
            return DeltaTable.forPath(spark, tablePath);
        else {
            logger.warn("Cannot update manifest for table: {} - Not a delta table", tablePath);
        }
        return null;
    }

    public void endTableUpdates(final SparkSession spark, final TableIdentifier tableId) throws DataStorageException {
        final DeltaTable deltaTable = getTable(spark, tableId.toPath());
        updateManifest(deltaTable);
    }

    protected void updateManifest(final DeltaTable dt) {
        dt.generate("symlink_format_manifest");
    }

    public void updateDeltaManifestForTable(final SparkSession spark, final String tablePath) {
        final DeltaTable deltaTable = getTable(spark, tablePath);
        updateManifest(deltaTable);
    }
}
