package uk.gov.justice.digital.zone;

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.*;
import static uk.gov.justice.digital.zone.RawZone.PRIMARY_KEY_NAME;

public interface Fixtures {

    String RAW_PATH = "s3://loadjob/raw";
    String CURATED_PATH = "s3://loadjob/curated";
    String STRUCTURED_PATH = "s3://loadjob/structured";
    String VIOLATIONS_PATH = "s3://loadjob/violations";

    String TABLE_SOURCE = "oms_owner";
    String TABLE_NAME = "agency_internal_locations";
    String TABLE_OPERATION = "load";
    String ROW_CONVERTER = "row_converter";

    String RECORD_KEY_1 = "record-1";
    String RECORD_KEY_2 = "record-2";
    String RECORD_KEY_3 = "record-3";
    String RECORD_KEY_4 = "record-4";
    String RECORD_KEY_5 = "record-5";
    String RECORD_KEY_6 = "record-6";
    String RECORD_KEY_7 = "record-7";

    String PRIMARY_KEY_PLACEHOLDER = "<PRIMARY-KEY>";

    String PRIMARY_KEY_FIELD = "primary-key";

    String STRING_FIELD_KEY = "string-key";
    String STRING_FIELD_VALUE = "stringValue";

    String NULL_FIELD_KEY = "null-key";
    String NUMBER_FIELD_KEY = "number-key";
    Float NUMBER_FIELD_VALUE = 1.0F;
    String ARRAY_FIELD_KEY = "array-key";
    int[] ARRAY_FIELD_VALUE = { 1, 2, 3 };

    String JSON_DATA =
            "{" +
                    "\"" + PRIMARY_KEY_FIELD + "\": \"" + PRIMARY_KEY_PLACEHOLDER + "\"," +
                    "\"" + STRING_FIELD_KEY + "\": \"" + STRING_FIELD_VALUE + "\"," +
                    "\"" + NULL_FIELD_KEY + "\": null," +
                    "\"" + NUMBER_FIELD_KEY + "\": " + NUMBER_FIELD_VALUE + "," +
                    "\"" + ARRAY_FIELD_KEY + "\": " + Arrays.toString(ARRAY_FIELD_VALUE) +
            "}";

    String recordData1 = JSON_DATA.replaceAll(PRIMARY_KEY_PLACEHOLDER, RECORD_KEY_1);
    String recordData2 = JSON_DATA.replaceAll(PRIMARY_KEY_PLACEHOLDER, RECORD_KEY_2);
    String recordData3 = JSON_DATA.replaceAll(PRIMARY_KEY_PLACEHOLDER, RECORD_KEY_3);
    String recordData4 = JSON_DATA.replaceAll(PRIMARY_KEY_PLACEHOLDER, RECORD_KEY_4);
    String recordData5 = JSON_DATA.replaceAll(PRIMARY_KEY_PLACEHOLDER, RECORD_KEY_5);
    String recordData6 = JSON_DATA.replaceAll(PRIMARY_KEY_PLACEHOLDER, RECORD_KEY_6);
    String recordData7 = JSON_DATA.replaceAll(PRIMARY_KEY_PLACEHOLDER, RECORD_KEY_7);

    StructType JSON_DATA_SCHEMA = new StructType()
            .add(PRIMARY_KEY_FIELD, DataTypes.StringType, false)
            .add(STRING_FIELD_KEY, DataTypes.StringType, false)
            .add(NULL_FIELD_KEY, DataTypes.StringType, true)
            .add(NUMBER_FIELD_KEY, DataTypes.FloatType, false)
            .add(ARRAY_FIELD_KEY, new ArrayType(DataTypes.IntegerType, false), false);

    StructType STRUCTURED_RECORD_WITH_OPERATION_SCHEMA = JSON_DATA_SCHEMA
            .add(OPERATION, DataTypes.StringType, false);

    String GENERIC_METADATA = "{}";
    String GENERIC_TIMESTAMP = "1";
    String GENERIC_KEY = "row_key";

    StructType ROW_SCHEMA = new StructType()
            .add(TIMESTAMP, DataTypes.StringType, false)
            .add(KEY, DataTypes.StringType, false)
            .add(SOURCE, DataTypes.StringType, false)
            .add(TABLE, DataTypes.StringType, false)
            .add(OPERATION, DataTypes.StringType, false)
            .add(CONVERTER, DataTypes.StringType, false)
            .add(RAW, DataTypes.StringType, false)
            .add(DATA, DataTypes.StringType, true)
            .add(METADATA, DataTypes.StringType, false);

    GenericRowWithSchema dataMigrationEventRow = new GenericRowWithSchema(
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


    StructType EXPECTED_RAW_SCHEMA = new StructType()
            .add(PRIMARY_KEY_NAME, DataTypes.StringType, false)
            .add(TIMESTAMP, DataTypes.StringType, false)
            .add(KEY, DataTypes.StringType, false)
            .add(SOURCE, DataTypes.StringType, false)
            .add(TABLE, DataTypes.StringType, false)
            .add(OPERATION, DataTypes.StringType, false)
            .add(CONVERTER, DataTypes.StringType, false)
            .add(RAW, DataTypes.StringType, false);

}
