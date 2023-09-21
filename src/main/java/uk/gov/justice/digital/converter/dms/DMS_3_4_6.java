package uk.gov.justice.digital.converter.dms;

import jakarta.inject.Inject;
import lombok.val;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.converter.Converter;
import uk.gov.justice.digital.provider.SparkSessionProvider;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.*;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.*;

/**
 * Converter that takes raw data from DMS v3.4.6 and converts it into the standardised data representation for
 * onward processing. See Converter.PARSED_DATA_SCHEMA
 * <p>
 * This converter will fail with an exception if
 *   o invalid json is encountered that cannot be parsed
 *   o after parsing, any of the fields in the standard format are null
 */
@Singleton
@Named("converterForDMS_3_4_6")
public class DMS_3_4_6 implements Converter<JavaRDD<Row>, Dataset<Row>> {

    public static final String CONVERTER_VERSION = "dms:3.4.6";
    public static final String ORIGINAL = "original";
    public static final String JSON_DATA = "jsonData";

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

        Operation(String name) { this.name = name; }

        public String getName() {
            return name;
        }

        public static Optional<Operation> getOperation(String operation) {
            return Arrays.stream(values()).filter(it -> it.name().equalsIgnoreCase(operation)).findAny();
        }

        public static final Object[] cdcOperations = { Insert.getName(), Update.getName(), Delete.getName() };
    }

    // This schema defines the common output format to be created from the incoming data.
    protected static final StructType PARSED_DATA_SCHEMA = new StructType()
            .add(RAW, StringType, NOT_NULL)
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


    private static final StructType eventsSchema =
        new StructType()
            .add(ORIGINAL, StringType);

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

    // Allow parse errors to fail silently when parsing the raw JSON string to allow control records to be filtered.
    private static final Map<String, String> jsonOptions = Collections.singletonMap("mode", "PERMISSIVE");

    private final SparkSession spark;

    @Inject
    public DMS_3_4_6(
            JobArguments arguments,
            SparkSessionProvider sparkSessionProvider
    ) {
        this.spark = sparkSessionProvider.getConfiguredSparkSession(arguments.getLogLevel());
    }

    public Dataset<Row> convert(Dataset<Row> inputDf) {
//        val df =
//                spark.createDataFrame(rdd, eventsSchema)
//                .withColumn(RAW, col(ORIGINAL))
//                .withColumn(JSON_DATA, from_json(col(ORIGINAL), RECORD_SCHEMA, jsonOptions))
//                 .select(RAW,JSON_DATA + ".*")
        val df = inputDf
//                .select(RAW, DATA, METADATA, METADATA + ".*")
                // Construct a dataframe that aligns to the parsed data schema
                .select(
                        lit("").as(RAW), // TODO would need to get raw back with json func
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
                                .otherwise(md5(col(RAW)))
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

    @Override
    public Dataset<Row> convert(JavaRDD<Row> rdd) {
        val df = spark.createDataFrame(rdd, eventsSchema)
            .withColumn(RAW, col(ORIGINAL))
            .withColumn(JSON_DATA, from_json(col(ORIGINAL), RECORD_SCHEMA, jsonOptions))
            .select(RAW,JSON_DATA + ".*")
            .select(RAW, DATA, METADATA, METADATA + ".*")
            // Construct a dataframe that aligns to the parsed data schema
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
                            .otherwise(md5(col(RAW)))
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
