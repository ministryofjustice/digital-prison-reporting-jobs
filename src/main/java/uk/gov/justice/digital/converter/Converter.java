package uk.gov.justice.digital.converter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static uk.gov.justice.digital.converter.Converter.ParsedDataFields.*;

public abstract class Converter {

    private static final boolean NOT_NULL = false;

    // TODO - at the moment we haven't really formalised the idea of our internal format which we could do instead of
    //        defining the fields and the struct here.

    // Constants defining the fields used in the common output format.
    public static class ParsedDataFields {
        public static final String DATA = "data";
        public static final String METADATA = "metadata";
        public static final String OPERATION = "operation";
        public static final String SOURCE = "source";
        public static final String TABLE = "table";
    }

    // This schema defines the common output format to be created from the incoming data.
    protected static final StructType PARSED_DATA_SCHEMA = new StructType()
        .add(DATA, StringType, NOT_NULL)
        .add(METADATA, StringType, NOT_NULL)
        .add(SOURCE, StringType, NOT_NULL)
        .add(TABLE, StringType, NOT_NULL)
        .add(OPERATION, StringType, NOT_NULL);

    public abstract Dataset<Row> convert(JavaRDD<Row> input, SparkSession spark);

}
