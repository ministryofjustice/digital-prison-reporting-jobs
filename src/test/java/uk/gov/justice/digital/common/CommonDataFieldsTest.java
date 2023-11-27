package uk.gov.justice.digital.common;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.TIMESTAMP;
import static uk.gov.justice.digital.common.CommonDataFields.withMetadataFields;

class CommonDataFieldsTest {

    @Test
    public void shouldAddMetaDataFields() {
        StructType originalSchema = new StructType(new StructField[]{
                new StructField("mycolumn", DataTypes.IntegerType, false, Metadata.empty())
        });


        StructType expectedResultSchema = new StructType(new StructField[]{
                new StructField("mycolumn", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField(OPERATION, DataTypes.StringType, false, Metadata.empty()),
                new StructField(TIMESTAMP, DataTypes.StringType, false, Metadata.empty()),
        });

        StructType resultSchema = withMetadataFields(originalSchema);

        assertEquals(expectedResultSchema, resultSchema);
    }

}