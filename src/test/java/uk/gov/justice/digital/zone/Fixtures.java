package uk.gov.justice.digital.zone;

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static uk.gov.justice.digital.converter.Converter.ParsedDataFields.*;

public class Fixtures {

    public static final String RAW_PATH = "s3://loadjob/raw";
    public static final String CURATED_PATH = "s3://loadjob/curated";
    public static final String STRUCTURED_PATH = "s3://loadjob/structured";
    public static final String VIOLATIONS_PATH = "s3://loadjob/violations";

    public static final String TABLE_SOURCE = "oms_owner";
    public static final String TABLE_NAME = "agency_internal_locations";
    public static final String TABLE_OPERATION = "load";

    public static final StructType ROW_SCHEMA = new StructType()
            .add(SOURCE, StringType, false)
            .add(TABLE, StringType, false)
            .add(OPERATION, StringType, false);

    public static final GenericRowWithSchema dataMigrationEventRow = new GenericRowWithSchema(
            new Object[] { TABLE_SOURCE, TABLE_NAME, TABLE_OPERATION },
            ROW_SCHEMA
        );

    private Fixtures() { }

}