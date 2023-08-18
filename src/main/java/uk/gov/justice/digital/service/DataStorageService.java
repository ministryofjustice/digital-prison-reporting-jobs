package uk.gov.justice.digital.service;

import io.delta.tables.DeltaTable;
import lombok.val;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.domain.model.TableIdentifier;
import uk.gov.justice.digital.exception.DataStorageException;

import javax.inject.Singleton;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

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
            val errorMessage = "Path " + tablePath + " is not set or dataframe is null";
            logger.error(errorMessage);
            throw new DataStorageException(errorMessage);
        }
    }

    public void appendDistinct(String tablePath, Dataset<Row> df, SourceReference.PrimaryKey primaryKey) throws DataStorageException {
        if(!df.isEmpty()) {
            val dt = getTable(df.sparkSession(), tablePath);
            if(dt.isPresent()) {
                val condition = primaryKey.getSparkCondition(SOURCE, TARGET);
                dt.get().as(SOURCE)
                        .merge(df.as(TARGET), condition )
                        .whenNotMatched().insertAll()
                        .execute();
            } else {
                append(tablePath, df);
            }
        }
    }

    public void updateRecords(
            SparkSession spark,
            String tablePath,
            Dataset<Row> dataFrame,
            SourceReference.PrimaryKey primaryKey) throws DataStorageException {
        val dt = getTable(spark, tablePath);

        if (dt.isPresent() && dataFrame != null) {
            val condition = primaryKey.getSparkCondition(SOURCE, TARGET);
            dt.get().as(SOURCE)
                    .merge(dataFrame.as(TARGET), condition)
                    .whenMatched()
                    .updateAll()
                    .execute();
        } else {
            val errorMessage = "Failed to access Delta table for update";
            logger.error(errorMessage);
            throw new DataStorageException(errorMessage);
        }
    }

    public void deleteRecords(
            SparkSession spark,
            String tablePath,
            Dataset<Row> dataFrame,
            SourceReference.PrimaryKey primaryKey) throws DataStorageException {
        val dt = getTable(spark, tablePath);

        if (dt.isPresent() && dataFrame != null) {
            val condition = primaryKey.getSparkCondition(SOURCE, TARGET);
            dt.get().as(SOURCE)
                    .merge(dataFrame.as(TARGET), condition)
                    .whenMatched()
                    .delete()
                    .execute();
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
            val errorMessage = "Path " + tablePath + " is not set or dataframe is null";
            logger.error(errorMessage);
            throw new DataStorageException(errorMessage);
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
            val errorMessage = "Path " + tablePath + " is not set or dataframe is null";
            logger.error(errorMessage);
            throw new DataStorageException(errorMessage);
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
            val errorMessage = "Path " + tablePath + " is not set or dataframe is null";
            logger.error(errorMessage);
            throw new DataStorageException(errorMessage);
        }

    }

    public void delete(SparkSession spark, TableIdentifier tableId) throws DataStorageException {
        logger.info("Deleting Delta table {}.{}", tableId.getSchema(), tableId.getTable());

        getTable(spark, tableId.toPath())
                .orElseThrow(() -> new DataStorageException("Failed to delete table. Table does not exist"))
                .delete();
    }

    public void vacuum(SparkSession spark, TableIdentifier tableId) throws DataStorageException {
        logger.info("Vacuuming Delta table {}.{}", tableId.getSchema(), tableId.getTable());
        vacuum(spark, tableId.toPath());
    }

    /**
     * Run a delta lake vacuum operation on the delta table at the given tablePath.
     */
    public void vacuum(SparkSession spark, String tablePath) throws DataStorageException {
        logger.info("Vacuuming Delta table at {}", tablePath);
        getTable(spark, tablePath)
                .orElseThrow(() -> new DataStorageException("Failed to vacuum table. Table does not exist"))
                .vacuum();
        logger.info("Finished vacuuming Delta table at {}", tablePath);
    }

    public Dataset<Row> get(SparkSession spark, TableIdentifier tableId) {
        return get(spark, tableId.toPath());
    }

    public Dataset<Row> get(SparkSession spark, String tablePath) {
        return getTable(spark, tablePath).map(DeltaTable::toDF).orElse(spark.emptyDataFrame());
    }

    protected Optional<DeltaTable> getTable(SparkSession spark, String tablePath) {
        if (DeltaTable.isDeltaTable(spark, tablePath))
            return Optional.of(DeltaTable.forPath(spark, tablePath));
        else {
            logger.warn("No valid table found for path: {} - Not a delta table", tablePath);
            return Optional.empty();
        }
    }

    public void endTableUpdates(SparkSession spark, TableIdentifier tableId) throws DataStorageException {
        updateDeltaManifestForTable(spark, tableId.toPath());
    }

    protected void updateManifest(DeltaTable dt) {
        dt.generate("symlink_format_manifest");
    }

    public void updateDeltaManifestForTable(SparkSession spark, String tablePath) {
        logger.info("Updating manifest for table: {}", tablePath);

        val  deltaTable = getTable(spark, tablePath);

        if (deltaTable.isPresent()) updateManifest(deltaTable.get());
        else logger.warn("Unable to update manifest for table: {} Not a delta table", tablePath);
    }

    /**
     * Runs a delta lake compaction on the Delta table at the given tablePath
     */

    public void compactDeltaTable(SparkSession spark, String tablePath) throws DataStorageException {
        logger.info("Compacting table at path: {}", tablePath);
        val deltaTable = getTable(spark, tablePath);
        deltaTable
                .orElseThrow(() -> new DataStorageException(format("Failed to compact table at path %s. Table does not exist", tablePath)))
                .optimize().executeCompaction();
        logger.info("Finished compacting table at path: {}", tablePath);
    }

    /**
     * List all Delta table paths immediately below rootPath
     * @return The list of delta table paths (including hadoop filesystem prefix, e.g. s3://)
     */
    public List<String> listDeltaTablePaths(SparkSession spark, String rootPath) throws DataStorageException {
        logger.info("Listing all delta table paths below: {}", rootPath);
        try {
            val fs = FileSystem.get(new URI(rootPath), spark.sparkContext().hadoopConfiguration());
            val path = new Path(rootPath);
            return Arrays.stream(fs.listStatus(path))
                    .filter(FileStatus::isDirectory)
                    .map(FileStatus::getPath)
                    .map(Path::toUri)
                    .map(URI::toString)
                    .filter(tablePath -> DeltaTable.isDeltaTable(spark, tablePath))
                    .collect(Collectors.toList());
        } catch (URISyntaxException e) {
            val errorMessage = format("Badly formatted root path when listing delta tables: %s", rootPath);
            logger.error(errorMessage);
            throw new DataStorageException(errorMessage, e);
        } catch (IOException e) {
            val errorMessage = format("Exception encountered when listing delta tables at root path: %s", rootPath);
            logger.error(errorMessage);
            throw new DataStorageException(errorMessage, e);
        }
    }


}
