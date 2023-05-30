package uk.gov.justice.digital.converter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Optional;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static uk.gov.justice.digital.converter.Converter.ParsedDataFields.*;

public abstract class Converter {

    private static final boolean NOT_NULL = false;
    private static final boolean IS_NULL = true;

    // TODO - at the moment we haven't really formalised the idea of our internal format which we could do instead of
    //        defining the fields and the struct here.

    // Constants defining the fields used in the common output format.
    public static class ParsedDataFields {
        public static final String RAW = "raw"; // the raw incoming value, unchanged
        public static final String DATA = "data"; // the payload. Not always present
        public static final String METADATA = "metadata"; // the payload metadata. Always present

        // From the metadata. By default we use _ as a differentiator so that payloads with
        // metadata fields do not get overwritten
        public static final String TIMESTAMP = "_timestamp";
        public static final String KEY = "_key";
        public static final String DATA_TYPE = "_datatype";
        public static final String OPERATION = "_operation";
        public static final String SOURCE = "_source";
        public static final String TABLE = "_table";
        public static final String CONVERTER = "_converter";
    }

    public enum Operation {
        Load("load"),
        Insert("insert"),
        Update("update"),
        Delete("delete");

        private String name;
        Operation(String name) { this.name = name; }

        public String getName() {
            return name;
        }

        public static Optional<Operation> getOperation(String operation) {
            return Arrays.stream(values()).filter(it -> it.name().equalsIgnoreCase(operation)).findAny();
        }
    };

    // This schema defines the common output format to be created from the incoming data.
    protected static final StructType PARSED_DATA_SCHEMA = new StructType()
        .add(RAW, StringType, NOT_NULL)
        .add(DATA, StringType, IS_NULL)
        .add(METADATA, StringType, NOT_NULL)
        .add(TIMESTAMP, StringType, NOT_NULL)
        .add(KEY, StringType, NOT_NULL)
        .add(DATA_TYPE, StringType, NOT_NULL)
        .add(SOURCE, StringType, NOT_NULL)
        .add(TABLE, StringType, NOT_NULL)
        .add(OPERATION, StringType, NOT_NULL)
        .add(CONVERTER, StringType, NOT_NULL);

    public abstract Dataset<Row> convert(JavaRDD<Row> input, SparkSession spark);

}
