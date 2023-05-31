package uk.gov.justice.digital.service;

import io.delta.tables.DeltaTable;
import jakarta.inject.Inject;
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

import java.util.Optional;

@Singleton
public class DataStorageService {

    private static final String SOURCE = "source";
    private static final String TARGET = "target";

    private static final Logger logger = LoggerFactory.getLogger(DataStorageService.class);

    private final SparkSession spark;

    @Inject
    public DataStorageService(SparkSessionProvider sparkSessionProvider) {
        this.spark = sparkSessionProvider.getConfiguredSparkSession();
    }

    public boolean exists(TableIdentifier tableId) {
        val exists = DeltaTable.isDeltaTable(spark, tableId.toPath());
        logger.info("Delta table path {} {}", tableId.toPath(), (exists) ? "exists" : "does not exist");
        return exists;
    }

    public boolean hasRecords(TableIdentifier tableId) throws DataStorageException {
        val hasRecords = exists(tableId) &&
            Optional.ofNullable(get(tableId))
                .map(df -> !df.isEmpty())
                .orElse(false);

        logger.info("Delta table {} {}", tableId.getSchema(), (hasRecords) ? "contains data" : "is empty");

        return hasRecords;
    }

    public void append(String tablePath, Dataset<Row> df) {
        df.write()
                .format("delta")
                .mode("append")
                .option("path", tablePath)
                .save();
    }

    public void appendDistinct(String tablePath, Dataset<Row> df, String primaryKey) {
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

    public void create(String tablePath, Dataset<Row> df) {
        logger.info("Inserting schema and data to " + tablePath);
        df.write()
                .format("delta")
                .option("path", tablePath)
                .save();
    }

    public void replace(String tablePath, Dataset<Row> df) {
        logger.info("Overwriting schema and data to " + tablePath);
        df.write()
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", true)
                .option("path", tablePath)
                .save();
    }

    public void resync(String tablePath, Dataset<Row> df) {
        logger.info("Syncing data to " + tablePath);
        df.write()
                .format("delta")
                .mode("overwrite")
                .option("path", tablePath)
                .save();
    }

    public void delete(TableIdentifier tableId) throws DataStorageException {
        logger.info("Deleting Delta table {}.{}", tableId.getSchema(), tableId.getTable());

        val deltaTable = Optional.ofNullable(getTable(tableId.toPath()))
                .orElseThrow(() -> new DataStorageException("Failed to delete table. Table does not exist"));

        deltaTable.delete();
    }

    public void vacuum(TableIdentifier tableId) throws DataStorageException {
        val deltaTable = Optional.ofNullable(getTable(tableId.toPath()))
                .orElseThrow(() -> new DataStorageException("Failed to vacuum table. Table does not exist"));

        deltaTable.vacuum();
    }

    public Dataset<Row> get(TableIdentifier tableId) {
        return get(tableId.toPath());
    }

    public void endTableUpdates(TableIdentifier tableId) {
        updateDeltaManifestForTable(tableId.toPath());
    }

    public void updateDeltaManifestForTable(String tablePath) {
        val deltaTable = getTable(tablePath);
        updateManifest(deltaTable);
    }

    private void updateManifest(DeltaTable dt) {
        dt.generate("symlink_format_manifest");
    }

    private Dataset<Row> get(String tablePath) {
        DeltaTable deltaTable = getTable(tablePath);
        return deltaTable == null ? null : deltaTable.toDF();
    }

    private DeltaTable getTable(String tablePath) {
        if (DeltaTable.isDeltaTable(spark, tablePath))
            return DeltaTable.forPath(spark, tablePath);
        else {
            logger.error("No table found for path: {}", tablePath);
            return null;
        }
    }

}
