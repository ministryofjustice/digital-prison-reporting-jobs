package uk.gov.justice.digital.converter.dms;

import lombok.val;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import uk.gov.justice.digital.converter.Converter;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static uk.gov.justice.digital.common.ColumnNames.*;

/**
 * Converter that takes raw data from DMS v3.4.6 and converts it into the standardised data representation for
 * onward processing. See Converter.PARSED_DATA_SCHEMA
 *
 * This converter will fail with an exception if
 *   o invalid json is encountered that cannot be parsed
 *   o after parsing, any of the fields in the standard format are null
 */
@Singleton
@Named("converterForDMS_3_4_6")
public class DMS_3_4_6 extends Converter {

    private static final StructType eventsSchema =
        new StructType()
            .add(DATA, StringType);

    private static final StructType recordSchema =
        new StructType()
            .add(DATA, StringType) // Data varies per table, so we leave parsing to the StructuredZone.
            .add(METADATA, new StructType()
                .add("timestamp", StringType)
                .add("record-type", StringType)
                .add("operation", StringType)
                .add("partition-key-type", StringType)
                .add("partition-key-value", StringType)
                .add("schema-name", StringType)
                .add("table-name", StringType)
            );

    // Allow parse errors to fail silently when parsing the raw JSON string to allow control records to be filtered.
    private static final Map<String, String> jsonOptions = Collections.singletonMap("mode", "PERMISSIVE");

    @Override
    public Dataset<Row> convert(JavaRDD<Row> rdd, SparkSession spark) {
        val df = spark.createDataFrame(rdd, eventsSchema)
            .withColumn(JSON_DATA, from_json(col(DATA), recordSchema, jsonOptions))
            .drop(DATA)
            .select(JSON_DATA + ".*")
            .select(DATA, METADATA, METADATA + ".*")
            // Ignore control records since they contain no data
            .filter(lower(col("record-type")).notEqual("control"))
            // Construct a dataframe that aligns to the parsed data schema
            .select(
                col(DATA),
                to_json(col(METADATA)),
                lower(col("schema-name")).as(SOURCE),
                lower(col("table-name").as(TABLE)),
                lower(col("operation").as(OPERATION))
            );

        // Strictly apply the parsed data schema which will fail if any values are null.
        return spark.createDataFrame(df.javaRDD(), PARSED_DATA_SCHEMA);
    }

}
