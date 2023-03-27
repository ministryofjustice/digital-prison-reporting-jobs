package uk.gov.justice.digital.zone;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import uk.gov.justice.digital.service.model.SourceReference;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.job.model.Columns.*;

// TODO - probably better as an abstract class with protected methods
public interface Zone {

    void process(Dataset<Row> dataFrame);

    default String getTablePath(String prefix, SourceReference ref, String operation) {
        return getTablePath(prefix, ref.getSource(), ref.getTable(), operation);
    }

    default String getTablePath(String prefix, SourceReference ref) {
        return getTablePath(prefix, ref.getSource(), ref.getTable());
    }

    default String getTablePath(String... elements) {
        return String.join("/", elements);
    }

    // These rows are used to select only those records corresponding to a specific source and table that has loads
    // events in the batch being processed.
    default List<Row> getTablesWithLoadRecords(Dataset<Row> dataFrame) {
        return dataFrame
            .filter(col(OPERATION).equalTo("load"))
            .select(TABLE, SOURCE, OPERATION)
            .distinct()
            .collectAsList();
    }

    default void appendToDeltaLakeTable(Dataset<Row> dataFrame, String tablePath) {
        dataFrame
            .write()
            .mode(SaveMode.Append)
            .option("path", tablePath)
            .format("delta")
            .save();
    }

}
