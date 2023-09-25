package uk.gov.justice.digital.converter.dms;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import uk.gov.justice.digital.converter.Converter;

import java.util.Arrays;
import java.util.Optional;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.md5;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.CONVERTER;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.DATA;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.DATA_TYPE;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.KEY;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.METADATA;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.RAW;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.SOURCE;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.TABLE;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.TIMESTAMP;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.TRANSACTION_ID;

/**
 * Converter that takes raw data from DMS v3.4.6 and converts it into the standardised data representation for
 * onward processing. See Converter.PARSED_DATA_SCHEMA
 * <p>
 * This converter will fail with an exception if
 * o invalid json is encountered that cannot be parsed
 * o after parsing, any of the fields in the standard format are null
 */
public class DMS_3_4_6 implements Converter<Dataset<Row>, Dataset<Row>> {

    public static final String CONVERTER_VERSION = "dms:3.4.6";
    private static final boolean NOT_NULL = false;
    private static final boolean IS_NULLABLE = true;

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
        public static final String TRANSACTION_ID = "_txnid";

        public static final String CONVERTER = "_converter";
    }

    public enum Operation {
        Load("load"),
        Insert("insert"),
        Update("update"),
        Delete("delete");

        private final String name;

        Operation(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public static Optional<Operation> getOperation(String operation) {
            return Arrays.stream(values()).filter(it -> it.name().equalsIgnoreCase(operation)).findAny();
        }

        public static final Object[] cdcOperations = {Insert.getName(), Update.getName(), Delete.getName()};
    }

    public static final StructType RECORD_SCHEMA =
            new StructType()
                    .add(DATA, StringType, IS_NULLABLE) // Data varies per table, so we leave parsing to the StructuredZone.
                    .add(METADATA, new StructType()
                            .add("timestamp", StringType)
                            .add("record-type", StringType)
                            .add("operation", StringType)
                            .add("partition-key-type", StringType)
                            .add("partition-key-value", StringType)
                            .add("schema-name", StringType)
                            .add("table-name", StringType)
                            .add("transaction-id", StringType, IS_NULLABLE)
                    );

    // This schema defines the common output format to be created from the incoming data.
    protected static final StructType PARSED_DATA_SCHEMA = new StructType()
            .add(RAW, RECORD_SCHEMA, NOT_NULL)
            .add(DATA, StringType, IS_NULLABLE)
            .add(METADATA, StringType, NOT_NULL)
            .add(TIMESTAMP, StringType, NOT_NULL)
            .add(KEY, StringType, NOT_NULL)
            .add(DATA_TYPE, StringType, NOT_NULL)
            .add(SOURCE, StringType, NOT_NULL)
            .add(TABLE, StringType, NOT_NULL)
            .add(OPERATION, StringType, NOT_NULL)
            .add(TRANSACTION_ID, StringType, IS_NULLABLE)
            .add(CONVERTER, StringType, NOT_NULL);
    private final SparkSession spark;

    public DMS_3_4_6(SparkSession spark) {
        this.spark = spark;
    }

    @Override
    public Dataset<Row> convert(Dataset<Row> inputDf) {
        // Construct a dataframe that aligns to the parsed data schema
        val df = inputDf
                .select(
                        struct(col("*")).as(RAW),
                        col(DATA),
                        col(METADATA),
                        col(METADATA + ".*")
                )
                .select(
                        col(RAW),
                        col(DATA),
                        to_json(col(METADATA)).as(METADATA),
                        col("timestamp").as(TIMESTAMP),
                        // when there is a partition-key-value we should use it
                        (when(col("partition-key-value").isNotNull(), col("partition-key-value"))
                                // when not and there is an operation == insert, update, delete
                                .when(col("partition-key-value").isNull().and(expr("operation == 'insert'")),
                                        concat(col("schema-name"), lit("."), col("table-name"), lit("."), col("transaction-id")))

                                .when(col("partition-key-value").isNull().and(expr("operation == 'update'")),
                                        concat(col("schema-name"), lit("."), col("table-name"), lit("."), col("transaction-id")))

                                .when(col("partition-key-value").isNull().and(expr("operation == 'delete'")),
                                        concat(col("schema-name"), lit("."), col("table-name"), lit("."), col("transaction-id")))
                                // when it is something else and there is a transaction-id, use that
                                .when(col("transaction-id").isNotNull(), col("transaction-id"))
                                // otherwise we MD5 the raw as this SHOULD be unique
                                .otherwise(md5(to_json(col(RAW))))
                        ).as(KEY),
                        col("record-type").as(DATA_TYPE),
                        lower(col("schema-name")).as(SOURCE),
                        lower(col("table-name")).as(TABLE),
                        lower(col("operation")).as(OPERATION),
                        col("transaction-id").as(TRANSACTION_ID),
                        lit(CONVERTER_VERSION).as(CONVERTER)
                );
        // Strictly apply the parsed data schema which will fail if any values are null.
        return spark.createDataFrame(df.javaRDD(), PARSED_DATA_SCHEMA);
    }
}
