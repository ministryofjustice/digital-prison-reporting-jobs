package uk.gov.justice.digital.service.operationaldatastore;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.config.BaseSparkTest;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.TIMESTAMP;


class OperationalDataStoreDataTransformationTest extends BaseSparkTest {

    private static final StructType schema = new StructType(new StructField[]{
            new StructField("PK", DataTypes.StringType, true, Metadata.empty()),
            new StructField(TIMESTAMP, DataTypes.StringType, true, Metadata.empty()),
            new StructField(OPERATION, DataTypes.StringType, true, Metadata.empty()),
            new StructField("DATA", DataTypes.StringType, true, Metadata.empty())
    });

    private final OperationalDataStoreDataTransformation underTest = new OperationalDataStoreDataTransformation();

    @Test
    public void shouldNormaliseColumnNamesToLowercase() {
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "I", "some data"),
                RowFactory.create("pk2", "2023-11-13 10:49:28.123458", "U", "some other data")
        ), schema);

        Dataset<Row> result = underTest.transform(df, schema.fields());

        assertThat(result.columns(), not(hasItemInArray("PK")));
        assertThat(result.columns(), hasItemInArray("pk"));
        assertThat(result.columns(), not(hasItemInArray("DATA")));
        assertThat(result.columns(), hasItemInArray("data"));

        assertEquals(1, result.where(col("pk").contains("pk1")).count());
        assertEquals(1, result.where(col("pk").contains("pk2")).count());
        assertEquals(1, result.where(col("data").contains("some data")).count());
        assertEquals(1, result.where(col("data").contains("some other data")).count());
    }

    @Test
    public void shouldRemoveOperationAndTimestampColumns() {
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "I", "some data"),
                RowFactory.create("pk2", "2023-11-13 10:49:28.123458", "U", "some other data")
        ), schema);

        Dataset<Row> result = underTest.transform(df, schema.fields());

        assertThat(result.columns(), not(hasItemInArray(OPERATION)));
        assertThat(result.columns(), not(hasItemInArray(TIMESTAMP)));
        assertThat(result.columns(), not(hasItemInArray(OPERATION.toLowerCase())));
        assertThat(result.columns(), not(hasItemInArray(TIMESTAMP.toLowerCase())));
    }

    @Test
    public void shouldStripNullStringCharacters() {
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1\u0000", "2023-11-13 10:49:28.123458", "I", "some data"),
                RowFactory.create("pk2", "2023-11-13 10:49:28.123458", "U", "\u0000some\u0000 other\u0000 data\u0000")
        ), schema);

        Dataset<Row> result = underTest.transform(df, schema.fields());

        assertEquals(0, result.where(col("pk").contains("\u0000")).count());
        assertEquals(0, result.where(col("data").contains("\u0000")).count());

        assertEquals(1, result.where(col("pk").contains("pk1")).count());
        assertEquals(1, result.where(col("data").contains("some other data")).count());
    }

    // TODO: Unhappy path, e.g. provided schema and dataframe schema don't match

}