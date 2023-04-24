package uk.gov.justice.digital.service;

import io.delta.tables.DeltaTable;
import io.micronaut.context.annotation.Bean;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.domain.model.TableInfo;

@Bean
public class DataStorageService {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DataStorageService.class);

    public boolean exists(final TableInfo info) {
        return DeltaTable.isDeltaTable(getTablePath(info.getPrefix(), info.getSchema(), info.getTable()));
    }

    protected String getTablePath(String prefix, SourceReference ref, String operation) {
        return getTablePath(prefix, ref.getSource(), ref.getTable(), operation);
    }

    protected String getTablePath(String prefix, SourceReference ref) {
        return getTablePath(prefix, ref.getSource(), ref.getTable());
    }

    public String getTablePath(String... elements) {
        return String.join("/", elements);
    }

    public void append(final String tablePath, final Dataset<Row> df) {
        df.write()
                .format("delta")
                .mode("append")
                .option("path", tablePath)
                .save();
    }

    public void create(final String tablePath, final Dataset<Row> df) {
        df.write()
                .format("delta")
                .option("path", tablePath)
                .save();
    }

    public void replace(final String tablePath, final Dataset<Row> df) {
        df.write()
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", true)
                .option("path", tablePath)
                .save();
    }

    public void reload(final String tablePath, final Dataset<Row> df) {
        df.write()
                .format("delta")
                .mode("overwrite")
                .option("path", tablePath)
                .save();
    }

    public void delete(final TableInfo info) {
        logger.info("deleting table...");
        String tablePath = getTablePath(info.getPrefix(), info.getSchema(), info.getTable());
        final DeltaTable deltaTable = getTable(tablePath);
        if(deltaTable != null) {
            deltaTable.delete();
        }
    }

    public void vacuum(final TableInfo info) {
        String tablePath = getTablePath(info.getPrefix(), info.getSchema(), info.getTable());
        final DeltaTable deltaTable = getTable(tablePath);
        if(deltaTable != null) {
            deltaTable.vacuum();
        }
    }

    public Dataset<Row> load(final TableInfo info) {
        String tablePath = getTablePath(info.getPrefix(), info.getSchema(), info.getTable());
        final DeltaTable deltaTable = getTable(tablePath);
        return deltaTable == null ? null : deltaTable.toDF();
    }

    protected DeltaTable getTable(final String tablePath) {
        if(DeltaTable.isDeltaTable(tablePath))
            return DeltaTable.forPath(tablePath);
        else {
            logger.warn("Cannot update manifest for table: {} - Not a delta table", tablePath);
        }
        return null;
    }

    public void endTableUpdates(final TableInfo info) {
        String tablePath = getTablePath(info.getPrefix(), info.getSchema(), info.getTable());
        final DeltaTable deltaTable = getTable(tablePath);
        updateManifest(deltaTable);
    }

    protected void updateManifest(final DeltaTable dt) {
        try {
            dt.generate("symlink_format_manifest");
        } catch(Exception e) {
            // TODO log error message
            // why are we here
        }
    }

    protected void updateDeltaManifestForTable(final String tablePath) {
        final DeltaTable deltaTable = getTable(tablePath);
        updateManifest(deltaTable);
    }
}
