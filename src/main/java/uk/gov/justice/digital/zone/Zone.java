package uk.gov.justice.digital.zone;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Zone {
    default String getTablePath(final String prefix, final String schema, final String table, String operation) {
        return prefix + "/" + schema + "/" + table + "/" + operation;
    }

    void process(Dataset<Row> rowRDD);

}
