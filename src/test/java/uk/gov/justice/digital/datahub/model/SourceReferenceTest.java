package uk.gov.justice.digital.datahub.model;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.Seq$;
import scala.jdk.CollectionConverters;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SourceReferenceTest {

    private SourceReference underTest;

    @BeforeEach
    void setUp() {
        underTest = new SourceReference(
                "key",
                "namespace",
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
    }

    @Test
    void shouldGetDataHubTableName() {
        String expectedFullTableName = "source.table";
        assertEquals(expectedFullTableName, underTest.getFullDatahubTableName());
    }

    @Test
    void shouldGetOperationalDataStoreTableName() {
        String expectedFullTableName = "source_table";
        assertEquals(expectedFullTableName, underTest.getOperationalDataStoreTableName());
    }

    @Test
    void shouldGetFullOperationalDataStoreTableNameWithSchema() {
        String expectedFullTableName = "namespace.source_table";
        assertEquals(expectedFullTableName, underTest.getFullOperationalDataStoreTableNameWithSchema());
    }

    @Test
    void shouldGetNamespace() {
        assertEquals("namespace", underTest.getNamespace());
    }

    @Test
    void shouldGetSparkKeyColumns() {
        Seq<Column> expectedKeyColumnNames = JavaConverters.asScalaBufferConverter(List.of(new Column("pk_column"))).asScala().toSeq();
        assertEquals(expectedKeyColumnNames, underTest.getPrimaryKey().getSparkKeyColumns());
    }

    @Test
    void shouldGetStringKeyColumnNames() {
        Seq<String> expectedKeyColumnNames = JavaConverters.asScalaBufferConverter(List.of("pk_column")).asScala().toSeq();
        assertEquals(expectedKeyColumnNames, underTest.getPrimaryKey().getStringKeyColumnNames());
    }
}
