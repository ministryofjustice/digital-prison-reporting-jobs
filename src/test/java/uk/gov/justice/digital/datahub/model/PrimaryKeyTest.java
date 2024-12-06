package uk.gov.justice.digital.datahub.model;

import org.apache.spark.sql.Column;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PrimaryKeyTest {

    @Test
    void shouldGetSparkConditionForSingleColumnPK() {
        String result = new SourceReference.PrimaryKey("pk_column").getSparkCondition("source", "target");
        String expected = "source.pk_column = target.pk_column";
        assertEquals(expected, result);
    }

    @Test
    void shouldGetSparkConditionForMultiColumnPK() {
        String result = new SourceReference.PrimaryKey(Arrays.asList("pk_column_1", "pk_column_2")).getSparkCondition("source", "target");
        String expected = "source.pk_column_1 = target.pk_column_1 and source.pk_column_2 = target.pk_column_2";
        assertEquals(expected, result);
    }

    @Test
    void shouldGetKeyColumnNamesForSingleColumnPK() {
        Collection<String> result = new SourceReference.PrimaryKey("pk_column").getKeyColumnNames();
        assertThat(result, contains("pk_column"));
    }

    @Test
    void shouldGetKeyColumnNamesForMultiColumnPK() {
        Collection<String> result = new SourceReference.PrimaryKey(Arrays.asList("pk_column_1", "pk_column_2")).getKeyColumnNames();
        assertThat(result, contains("pk_column_1", "pk_column_2"));
    }

    @Test
    void shouldGetSparkKeyColumnsForSingleColumnPK() {
        Seq<Column> result = new SourceReference.PrimaryKey("pk_column").getSparkKeyColumns();
        Seq<Column> expected = JavaConverters.asScalaBufferConverter(Collections.singletonList(new Column("pk_column"))).asScala().toSeq();
        assertEquals(expected, result);
    }

    @Test
    void shouldGetSparkKeyColumnsForMultiColumnPK() {
        Seq<Column> result = new SourceReference.PrimaryKey(Arrays.asList("pk_column_1", "pk_column_2")).getSparkKeyColumns();
        List<Column> javaColumns = Arrays.asList(new Column("pk_column_1"), new Column("pk_column_2"));
        Seq<Column> expected = JavaConverters.asScalaBufferConverter(javaColumns).asScala().toSeq();
        assertEquals(expected, result);
    }
}
