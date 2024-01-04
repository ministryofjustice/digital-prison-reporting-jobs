package uk.gov.justice.digital.test;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Fixtures {
    public static final String TABLE_NAME = "agency_internal_locations";
    public static final String PRIMARY_KEY_FIELD = "primary-key";
    public static final String STRING_FIELD_KEY = "string-key";
    public static final String NULL_FIELD_KEY = "null-key";
    public static final String NUMBER_FIELD_KEY = "number-key";
    public static final String ARRAY_FIELD_KEY = "array-key";

    public static final StructType JSON_DATA_SCHEMA = new StructType()
            .add(PRIMARY_KEY_FIELD, DataTypes.StringType, false)
            .add(STRING_FIELD_KEY, DataTypes.StringType, false)
            .add(NULL_FIELD_KEY, DataTypes.StringType, true)
            .add(NUMBER_FIELD_KEY, DataTypes.FloatType, false)
            .add(ARRAY_FIELD_KEY, new ArrayType(DataTypes.IntegerType, false), false);


    // Private constructor to prevent instantiation.
    private Fixtures() { }
}
