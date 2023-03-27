package uk.gov.justice.digital.zone;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.service.model.SourceReference;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.job.model.Columns.*;

public abstract class Zone {

    private static final Logger logger = LoggerFactory.getLogger(Zone.class);

    public abstract void process(Dataset<Row> dataFrame);

    protected String getTablePath(String prefix, SourceReference ref, String operation) {
        return getTablePath(prefix, ref.getSource(), ref.getTable(), operation);
    }

    protected String getTablePath(String prefix, SourceReference ref) {
        return getTablePath(prefix, ref.getSource(), ref.getTable());
    }

    protected String getTablePath(String... elements) {
        return String.join("/", elements);
    }

    // These rows are used to select only those records corresponding to a specific source and table that has loads
    // events in the batch being processed.
    protected List<Row> getTablesWithLoadRecords(Dataset<Row> dataFrame) {
        return dataFrame
            .filter(col(OPERATION).equalTo("load"))
            .select(TABLE, SOURCE, OPERATION)
            .distinct()
            .collectAsList();
    }

    protected void appendToDeltaLakeTable(Dataset<Row> dataFrame, String tablePath) {
        logger.info("Appending {} records to deltalake table: {}", dataFrame.count(), tablePath);

        dataFrame
            .write()
            .mode(SaveMode.Append)
            .option("path", tablePath)
            .format("delta")
            .save();

        logger.info("Append completed successfully");
    }

}
