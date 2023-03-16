package uk.gov.justice.digital.zone;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Zone {
    default String getTablePath(final String prefix, final String schema, final String table, String operation) {
        return prefix + "/" + schema + "/" + table + "/" + operation;
    }

    Dataset<Row> process(JavaRDD<Row> rowRDD, SparkSession spark);

}
