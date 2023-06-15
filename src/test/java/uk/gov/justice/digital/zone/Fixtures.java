package uk.gov.justice.digital.zone;

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.*;
import static uk.gov.justice.digital.zone.RawZone.PRIMARY_KEY_NAME;

public class Fixtures {

    public static final String RAW_PATH = "s3://loadjob/raw";
    public static final String CURATED_PATH = "s3://loadjob/curated";
    public static final String STRUCTURED_PATH = "s3://loadjob/structured";
    public static final String VIOLATIONS_PATH = "s3://loadjob/violations";

    public static final String TABLE_SOURCE = "oms_owner";
    public static final String TABLE_NAME = "agency_internal_locations";
    public static final String TABLE_OPERATION = "load";
    public static final String ROW_CONVERTER = "row_converter";
    public static final String RAW_DATA = "{}";

    public static final StructType ROW_SCHEMA = new StructType()
            .add(SOURCE, StringType, false)
            .add(TABLE, StringType, false)
            .add(OPERATION, StringType, false);

    public static final GenericRowWithSchema dataMigrationEventRow = new GenericRowWithSchema(
            new Object[] { TABLE_SOURCE, TABLE_NAME, TABLE_OPERATION },
            ROW_SCHEMA
        );

    public static final GenericRowWithSchema dataMigrationEventRowWithInvalidOperation = new GenericRowWithSchema(
            new Object[] { TABLE_SOURCE, TABLE_NAME, "makeTea" },
            ROW_SCHEMA
    );

    public static final StructType EXPECTED_RAW_SCHEMA = new StructType()
            .add(PRIMARY_KEY_NAME, DataTypes.StringType, false)
            .add(TIMESTAMP, DataTypes.StringType, false)
            .add(KEY, DataTypes.StringType, false)
            .add(SOURCE, DataTypes.StringType, false)
            .add(TABLE, DataTypes.StringType, false)
            .add(OPERATION, DataTypes.StringType, false)
            .add(CONVERTER, DataTypes.StringType, false)
            .add(RAW, DataTypes.StringType, false);

    public static final StructType RECORD_SCHEMA = new StructType() // TODO: Consolidate this and ROW_SCHEMA as part of DPR-310, DPR-314, DPR-311
            .add(TIMESTAMP, DataTypes.StringType, false)
            .add(KEY, DataTypes.StringType, false)
            .add(SOURCE, DataTypes.StringType, false)
            .add(TABLE, DataTypes.StringType, false)
            .add(OPERATION, DataTypes.StringType, false)
            .add(CONVERTER, DataTypes.StringType, false)
            .add(RAW, DataTypes.StringType, false);

    private Fixtures() { }

}
