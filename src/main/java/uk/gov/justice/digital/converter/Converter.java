package uk.gov.justice.digital.converter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Converter {
    Dataset<Row> convert(JavaRDD<Row> input, SparkSession spark);
}
