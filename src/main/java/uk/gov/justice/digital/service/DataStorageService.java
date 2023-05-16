package uk.gov.justice.digital.service;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.domain.model.TableIdentifier;
import uk.gov.justice.digital.exception.DataStorageException;

import javax.inject.Singleton;

@Singleton
public class DataStorageService {

    private static final Logger logger = LoggerFactory.getLogger(DataStorageService.class);

    public boolean exists(final SparkSession spark, final TableIdentifier info) {
        logger.info("Delta table path :" + getTablePath(info.getBasePath(), info.getSchema(), info.getTable()));
        return DeltaTable.isDeltaTable(spark, getTablePath(info.getBasePath(), info.getSchema(), info.getTable()));
    }

    public String getTablePath(String prefix, SourceReference ref, String operation) {
        return getTablePath(prefix, ref.getSource(), ref.getTable(), operation);
    }

    public String getTablePath(String prefix, SourceReference ref) {
        return getTablePath(prefix, ref.getSource(), ref.getTable());
    }

    public String getTablePath(String... elements) {
        return String.join("/", elements);
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

    public void delete(final SparkSession spark, final TableIdentifier info) throws DataStorageException {
        logger.info("deleting Delta table..." + info.getSchema() + "." + info.getTable());
        String tablePath = getTablePath(info.getBasePath(), info.getSchema(), info.getTable());
        final DeltaTable deltaTable = getTable(spark, tablePath);
        if (deltaTable != null) {
            deltaTable.delete();
        } else throw new DataStorageException("Delta table delete failed");
    }

    public void vacuum(final SparkSession spark, final TableIdentifier info) throws DataStorageException {
        String tablePath = getTablePath(info.getBasePath(), info.getSchema(), info.getTable());
        final DeltaTable deltaTable = getTable(spark, tablePath);
        if (deltaTable != null) {
            deltaTable.vacuum();
        } else throw new DataStorageException("Delta table vaccum failed");
    }

    public Dataset<Row> load(final SparkSession spark, final TableIdentifier info) throws DataStorageException {
        String tablePath = getTablePath(info.getBasePath(), info.getSchema(), info.getTable());
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

    public void endTableUpdates(final SparkSession spark, final TableIdentifier info) throws DataStorageException {
        String tablePath = getTablePath(info.getBasePath(), info.getSchema(), info.getTable());
        final DeltaTable deltaTable = getTable(spark, tablePath);
        updateManifest(deltaTable);
    }

    protected void updateManifest(final DeltaTable dt) {
        try {
            dt.generate("symlink_format_manifest");
        } catch (Exception e) {
            // TODO log error message
            // why are we here
        }
    }

    public void updateDeltaManifestForTable(final SparkSession spark, final String tablePath) {
        final DeltaTable deltaTable = getTable(spark, tablePath);
        updateManifest(deltaTable);
    }
}
