package uk.gov.justice.digital.service;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domains.model.TableInfo;

public class DataStorageService {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DataStorageService.class);

    public boolean exists(final TableInfo info) {
        return DeltaTable.isDeltaTable(getTablePath(info));
    }

    private String getTablePath(final TableInfo info) {
        return String.join("/", info.getPrefix(), info.getSchema(), info.getTable());
    }

    public void append(final TableInfo info, final Dataset<Row> df) {
        df.write()
                .format("delta")
                .mode("append")
                .option("path", getTablePath(info))
                .save();
    }

    public void replace(final TableInfo info, final Dataset<Row> df) {
        df.write()
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", true)
                .option("path", getTablePath(info))
                .save();
    }

    public void delete(final TableInfo info) {
        System.out.println("deleting table.");
        final DeltaTable deltaTable = getTable(info);
        if(deltaTable != null) {
            deltaTable.delete();
        }
    }

    public void vacuum(final TableInfo info) {
        final DeltaTable deltaTable = getTable(info);
        if(deltaTable != null) {
            deltaTable.vacuum();
        }
    }

    public Dataset<Row> load(final TableInfo info) {
        final DeltaTable deltaTable = getTable(info);
        return deltaTable == null ? null : deltaTable.toDF();
    }

    protected DeltaTable getTable(final TableInfo info) {
        if(DeltaTable.isDeltaTable(getTablePath(info)))
            return DeltaTable.forPath(getTablePath(info));
        else
            return null;
    }

    public void endTableUpdates(final TableInfo info) {
        final DeltaTable deltaTable = getTable(info);
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

}
