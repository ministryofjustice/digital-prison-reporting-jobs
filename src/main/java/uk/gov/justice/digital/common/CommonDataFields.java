package uk.gov.justice.digital.common;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class CommonDataFields {

    // The operation column added by DMS. See ShortOperationCode below.
    public static final String OPERATION = "Op";
    // The timestamp column added by AWS DMS.
    public static final String TIMESTAMP = "_timestamp";
    // The error column is added to the schema by the app when writing violations to give error details.
    public static final String ERROR = "error";
    public static final String ERROR_RAW = "raw";

    /**
     * The possible entries in the operation column
     */
    public enum ShortOperationCode {
        Insert("I"),
        Update("U"),
        Delete("D");

        private final String name;

        ShortOperationCode(String name) { this.name = name; }

        public String getName() {
            return name;
        }
    }

    private CommonDataFields() {}

    /**
     * Add the metadata fields to the provided schema
     */
    public static StructType withMetadataFields(StructType schema) {
        return schema
                .add(DataTypes.createStructField(OPERATION, DataTypes.StringType, false))
                .add(DataTypes.createStructField(TIMESTAMP, DataTypes.StringType, false));
    }
}
