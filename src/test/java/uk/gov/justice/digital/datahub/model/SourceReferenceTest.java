package uk.gov.justice.digital.datahub.model;

import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SourceReferenceTest {

    @Test
    void testGetFullyQualifiedTableName() {
        SourceReference underTest = new SourceReference(
                "key",
                "source",
                "table",
                new SourceReference.PrimaryKey("pk_column"),
                "versionId",
                new StructType(new StructField[]{
                        new StructField("pk_column", StringType$.MODULE$, true, null),
                        new StructField("some_sensitive_column", StringType$.MODULE$, true, null)
                }),
                new SourceReference.SensitiveColumns("some_sensitive_column")
        );

        String expectedFullTableName = "source.table";
        assertEquals(expectedFullTableName, underTest.getFullyQualifiedTableName());
    }
}