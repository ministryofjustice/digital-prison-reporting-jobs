package uk.gov.justice.digital.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.*;
import static uk.gov.justice.digital.zone.raw.RawZone.PRIMARY_KEY_NAME;

public class Fixtures {

    public static final String RAW_PATH = "s3://loadjob/raw";
    public static final String CURATED_PATH = "s3://loadjob/curated";
    public static final String STRUCTURED_PATH = "s3://loadjob/structured";
    public static final String VIOLATIONS_PATH = "s3://loadjob/violations";

    public static final String TABLE_SOURCE = "oms_owner";
    public static final String TABLE_NAME = "agency_internal_locations";
    public static final String TABLE_OPERATION = "load";
    public static final String ROW_CONVERTER = "row_converter";

    public static final String RECORD_KEY_1 = "record-1";
    public static final String RECORD_KEY_2 = "record-2";
    public static final String RECORD_KEY_3 = "record-3";
    public static final String RECORD_KEY_4 = "record-4";
    public static final String RECORD_KEY_5 = "record-5";
    public static final String RECORD_KEY_6 = "record-6";
    public static final String RECORD_KEY_7 = "record-7";

    public static final String PRIMARY_KEY_PLACEHOLDER = "<PRIMARY-KEY>";

    public static final String PRIMARY_KEY_FIELD = "primary-key";

    public static final String STRING_FIELD_KEY = "string-key";
    public static final String STRING_FIELD_VALUE = "stringValue";

    public static final String NULL_FIELD_KEY = "null-key";
    public static final String NUMBER_FIELD_KEY = "number-key";
    public static final Float NUMBER_FIELD_VALUE = 1.0F;
    public static final String ARRAY_FIELD_KEY = "array-key";
    public static final int[] ARRAY_FIELD_VALUE = { 1, 2, 3 };

    static String JSON_DATA =
            "{" +
                    "\"" + PRIMARY_KEY_FIELD + "\": \"" + PRIMARY_KEY_PLACEHOLDER + "\"," +
                    "\"" + STRING_FIELD_KEY + "\": \"" + STRING_FIELD_VALUE + "\"," +
                    "\"" + NULL_FIELD_KEY + "\": null," +
                    "\"" + NUMBER_FIELD_KEY + "\": " + NUMBER_FIELD_VALUE + "," +
                    "\"" + ARRAY_FIELD_KEY + "\": " + Arrays.toString(ARRAY_FIELD_VALUE) +
            "}";

    public static final String recordData1 = JSON_DATA.replaceAll(PRIMARY_KEY_PLACEHOLDER, RECORD_KEY_1);
    public static final String recordData2 = JSON_DATA.replaceAll(PRIMARY_KEY_PLACEHOLDER, RECORD_KEY_2);
    public static final String recordData3 = JSON_DATA.replaceAll(PRIMARY_KEY_PLACEHOLDER, RECORD_KEY_3);
    public static final String recordData4 = JSON_DATA.replaceAll(PRIMARY_KEY_PLACEHOLDER, RECORD_KEY_4);
    public static final String recordData5 = JSON_DATA.replaceAll(PRIMARY_KEY_PLACEHOLDER, RECORD_KEY_5);
    public static final String recordData6 = JSON_DATA.replaceAll(PRIMARY_KEY_PLACEHOLDER, RECORD_KEY_6);
    public static final String recordData7 = JSON_DATA.replaceAll(PRIMARY_KEY_PLACEHOLDER, RECORD_KEY_7);

    public static final StructType JSON_DATA_SCHEMA = new StructType()
            .add(PRIMARY_KEY_FIELD, DataTypes.StringType, false)
            .add(STRING_FIELD_KEY, DataTypes.StringType, false)
            .add(NULL_FIELD_KEY, DataTypes.StringType, true)
            .add(NUMBER_FIELD_KEY, DataTypes.FloatType, false)
            .add(ARRAY_FIELD_KEY, new ArrayType(DataTypes.IntegerType, false), false);

    public static final StructType STRUCTURED_RECORD_WITH_OPERATION_SCHEMA = JSON_DATA_SCHEMA
            .add(OPERATION, DataTypes.StringType, false);

    public static final String GENERIC_METADATA = "{}";
    public static final String GENERIC_TIMESTAMP = "1";
    public static final String GENERIC_KEY = "row_key";

    public static final StructType ROW_SCHEMA = new StructType()
            .add(TIMESTAMP, DataTypes.StringType, false)
            .add(KEY, DataTypes.StringType, false)
            .add(SOURCE, DataTypes.StringType, false)
            .add(TABLE, DataTypes.StringType, false)
            .add(OPERATION, DataTypes.StringType, false)
            .add(CONVERTER, DataTypes.StringType, false)
            .add(RAW, DataTypes.StringType, false)
            .add(DATA, DataTypes.StringType, true)
            .add(METADATA, DataTypes.StringType, false);

    public static final GenericRowWithSchema dataMigrationEventRow = new GenericRowWithSchema(
            new Object[] {
                    GENERIC_TIMESTAMP,
                    GENERIC_KEY,
                    TABLE_SOURCE,
                    TABLE_NAME,
                    TABLE_OPERATION,
                    ROW_CONVERTER,
                    JSON_DATA,
                    JSON_DATA,
                    GENERIC_METADATA
            },
            ROW_SCHEMA
    );


    public static StructType EXPECTED_RAW_SCHEMA = new StructType()
            .add(PRIMARY_KEY_NAME, DataTypes.StringType, false)
            .add(TIMESTAMP, DataTypes.StringType, false)
            .add(KEY, DataTypes.StringType, false)
            .add(SOURCE, DataTypes.StringType, false)
            .add(TABLE, DataTypes.StringType, false)
            .add(OPERATION, DataTypes.StringType, false)
            .add(CONVERTER, DataTypes.StringType, false)
            .add(RAW, DataTypes.StringType, false);

    public static List<Row> getAllCapturedRecords(ArgumentCaptor<Dataset<Row>> dataframeCaptor) {
        return dataframeCaptor
                .getAllValues()
                .stream()
                .flatMap(x -> x.collectAsList().stream())
                .collect(Collectors.toList());
    }

    public static boolean hasNullColumns(Dataset<Row> df) {
        for (String c : df.columns()) {
            if (df.filter(col(c).isNull()).count() > 0)
                return true;
        }
        return false;
    }

    // Private constructor to prevent instantiation.
    private Fixtures() { }
}
