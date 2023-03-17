package uk.gov.justice.digital.zone;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Zone {

    default String getTablePath(String prefix, String schema, String table, String operation) {
        return String.join("/", prefix, schema, table, operation);
    }

    void process(Dataset<Row> rowRDD);

}
