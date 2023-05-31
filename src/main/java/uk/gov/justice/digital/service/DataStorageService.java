package uk.gov.justice.digital.service;

import io.delta.tables.DeltaTable;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domain.model.TableIdentifier;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.provider.SparkSessionProvider;

@Singleton
public class DataStorageService {

    private static final String SOURCE = "source";
    private static final String TARGET = "target";

    private static final Logger logger = LoggerFactory.getLogger(DataStorageService.class);

    private final SparkSession spark;

    public DataStorageService(SparkSessionProvider sparkSessionProvider) {
        this.spark = sparkSessionProvider.getConfiguredSparkSession();
    }

    public boolean exists(TableIdentifier tableId) {
        val tablePath = tableId.toPath();
        logger.info("Delta table path {}", tablePath);
        return DeltaTable.isDeltaTable(spark, tablePath);
    }

    public boolean hasRecords(TableIdentifier info) throws DataStorageException {
        logger.info("Checking details for Delta table..." + info.getSchema() + "." + info.getTable());
        if (exists(info)) {
            Dataset<Row> df = get(info);
            return !df.isEmpty();
        } else {
            return false;
        }
    }

    public void append(String tablePath, Dataset<Row> df) throws DataStorageException {
        logger.info("Appending schema and data to " + tablePath);
        if (tablePath != null && df != null)
            df.write()
                    .format("delta")
                    .mode("append")
                    .option("path", tablePath)
                    .save();
        else throw new DataStorageException("Path is not set or dataframe is null");
    }

    public void appendDistinct(String tablePath, Dataset<Row> df, String primaryKey) throws DataStorageException {
        if(!df.isEmpty()) {
            final DeltaTable dt = getTable(tablePath);
            if(dt != null) {
                final String spk = SOURCE + "." + primaryKey;
                final String tpk = TARGET + "." + primaryKey;
                dt.as(SOURCE)
                        .merge(df.as(TARGET), spk + "=" + tpk )
                        .whenNotMatched().insertAll()
                        .execute();
            } else {
                append(tablePath, df);
            }
        }
    }

    public void create(String tablePath, Dataset<Row> df) throws DataStorageException {
        logger.info("Inserting schema and data to " + tablePath);
        if (tablePath != null && df != null)
            df.write()
                    .format("delta")
                    .option("path", tablePath)
                    .save();
        else throw new DataStorageException("Path is not set or dataframe is null");
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
        else throw new DataStorageException("Path is not set or dataframe is null");
    }

    public void resync(String tablePath, Dataset<Row> df) throws DataStorageException {
        logger.info("Syncing data to " + tablePath);
        if (tablePath != null && df != null)
            df.write()
                    .format("delta")
                    .mode("overwrite")
                    .option("path", tablePath)
                    .save();
        else throw new DataStorageException("Path is not set or dataframe is null");
    }

    public void delete(TableIdentifier tableId) throws DataStorageException {
        logger.info("Deleting Delta table..." + tableId.getSchema() + "." + tableId.getTable());
        val deltaTable = getTable(tableId.toPath());
        if (deltaTable != null) {
            deltaTable.delete();
        } else throw new DataStorageException("Delta table delete failed");
    }

    public void vacuum(TableIdentifier tableId) throws DataStorageException {
        val deltaTable = getTable(tableId.toPath());
        if (deltaTable != null) {
            deltaTable.vacuum();
        } else throw new DataStorageException("Delta table vaccum failed");
    }

    public Dataset<Row> get(TableIdentifier tableId) {
        return get(tableId.toPath());
    }

    public Dataset<Row> get(String tablePath) {
        DeltaTable deltaTable = getTable(tablePath);
        return deltaTable == null ? null : deltaTable.toDF();
    }

    // TODO - review log message
    protected DeltaTable getTable(String tablePath) {
        if (DeltaTable.isDeltaTable(spark, tablePath))
            return DeltaTable.forPath(spark, tablePath);
        else {
            logger.warn("Cannot update manifest for table: {} - Not a delta table", tablePath);
        }
        return null;
    }

    public void endTableUpdates(TableIdentifier tableId) {
        DeltaTable deltaTable = getTable(tableId.toPath());
        updateManifest(deltaTable);
    }

    protected void updateManifest(DeltaTable dt) {
        dt.generate("symlink_format_manifest");
    }

    public void updateDeltaManifestForTable(String tablePath) {
        DeltaTable deltaTable = getTable(tablePath);
        updateManifest(deltaTable);
    }

}
