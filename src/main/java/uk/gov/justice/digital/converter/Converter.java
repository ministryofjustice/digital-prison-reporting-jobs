package uk.gov.justice.digital.converter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static uk.gov.justice.digital.job.model.Columns.*;

public abstract class Converter {

    // This schema defines the common output format to be created from the incoming data.
    protected static final StructType PARSED_DATA_SCHEMA = new StructType()
        .add(DATA, StringType, false)
        .add(SOURCE, StringType, false)
        .add(TABLE, StringType, false)
        .add(OPERATION, StringType, false);

    public abstract Dataset<Row> convert(JavaRDD<Row> input, SparkSession spark);

}
