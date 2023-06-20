package uk.gov.justice.digital.converter.dms;

import lombok.val;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.config.BaseSparkTest;
import java.util.Arrays;
import uk.gov.justice.digital.provider.SparkSessionProvider;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.*;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.*;

class DMS_3_4_6_ConverterIntegrationTest extends BaseSparkTest {

    private static final String DATA_PATH = "src/it/resources/data/dms_record.json";
    private static final String CONTROL_PATH = "src/it/resources/data/null-data-dms-record.json";

    private static final String UPDATE_PATH = "src/it/resources/data/dms_update.json";
    private static final DMS_3_4_6 underTest = new DMS_3_4_6(new SparkSessionProvider());

    @Test
    void shouldConvertValidDataCorrectly() {
        // Load JSON as text and slurp in the context of the whole file so we can read multiline JSON files.
        val rdd = getData(DATA_PATH);

        val converted = underTest.convert(rdd);

        // Strict schema validation is applied so checking accounts agree should be sufficient here.
        assertEquals(rdd.count(), converted.count());
    }

    @Test
    void shouldConvertARawDataRecordIntoTheCorrectColumns() {
        val rdd = getData(DATA_PATH);

        val converted = underTest.convert(rdd);
        converted.show(false);

        val columnNames = Arrays.asList(converted.columns());

        assertTrue(columnNames.contains(RAW));
        assertTrue(columnNames.contains(DATA));
        assertTrue(columnNames.contains(METADATA));
        assertTrue(columnNames.contains(TIMESTAMP));
        assertTrue(columnNames.contains(KEY));
        assertTrue(columnNames.contains(DATA_TYPE));
        assertTrue(columnNames.contains(SOURCE));
        assertTrue(columnNames.contains(TABLE));
        assertTrue(columnNames.contains(OPERATION));
        assertTrue(columnNames.contains(TRANSACTION_ID));
        assertTrue(columnNames.contains(CONVERTER));

        assertFalse(converted.isEmpty());

        val row = converted.first();

        assertNotNull(row);

        assertNotNull(row.getAs(RAW));
        assertNotNull(row.getAs(DATA));
        assertNotNull(row.getAs(METADATA));
        assertNotNull(row.getAs(TIMESTAMP));
        assertNotNull(row.getAs(KEY));
        assertNotNull(row.getAs(DATA_TYPE));
        assertNotNull(row.getAs(SOURCE));
        assertNotNull(row.getAs(TABLE));
        assertNotNull(row.getAs(OPERATION));
        assertNull(row.getAs(TRANSACTION_ID));
        assertNotNull(row.getAs(CONVERTER));

    }

    @Test
    void shouldConvertARawControlRecordIntoTheCorrectColumns() {
        val rdd = getData(CONTROL_PATH);

        val converted = underTest.convert(rdd);
        converted.show(false);

        val columnNames = Arrays.asList(converted.columns());

        assertTrue(columnNames.contains(RAW));
        assertTrue(columnNames.contains(DATA));
        assertTrue(columnNames.contains(METADATA));
        assertTrue(columnNames.contains(TIMESTAMP));
        assertTrue(columnNames.contains(KEY));
        assertTrue(columnNames.contains(DATA_TYPE));
        assertTrue(columnNames.contains(SOURCE));
        assertTrue(columnNames.contains(TABLE));
        assertTrue(columnNames.contains(OPERATION));
        assertTrue(columnNames.contains(TRANSACTION_ID));
        assertTrue(columnNames.contains(CONVERTER));

        assertFalse(converted.isEmpty());

        val row = converted.first();

        assertNotNull(row);

        assertNotNull(row.getAs(RAW));
        assertNull(row.getAs(DATA));
        assertNotNull(row.getAs(METADATA));
        assertNotNull(row.getAs(TIMESTAMP));
        assertNotNull(row.getAs(KEY));
        assertNotNull(row.getAs(DATA_TYPE));
        assertNotNull(row.getAs(SOURCE));
        assertNotNull(row.getAs(TABLE));
        assertNotNull(row.getAs(OPERATION));
        assertNull(row.getAs(TRANSACTION_ID));
        assertNotNull(row.getAs(CONVERTER));

    }

    @Test
    void shouldConvertAnUpdateRecordIntoTheCorrectColumns() {
        val rdd = getData(UPDATE_PATH);

        val converted = underTest.convert(rdd);

        converted.show(false);

        val columnNames = Arrays.asList(converted.columns());

        assertTrue(columnNames.contains(RAW));
        assertTrue(columnNames.contains(DATA));
        assertTrue(columnNames.contains(METADATA));
        assertTrue(columnNames.contains(TIMESTAMP));
        assertTrue(columnNames.contains(KEY));
        assertTrue(columnNames.contains(DATA_TYPE));
        assertTrue(columnNames.contains(SOURCE));
        assertTrue(columnNames.contains(TABLE));
        assertTrue(columnNames.contains(OPERATION));
        assertTrue(columnNames.contains(TRANSACTION_ID));
        assertTrue(columnNames.contains(CONVERTER));

        assertFalse(converted.isEmpty());

        val row = converted.first();

        assertNotNull(row);

        assertNotNull(row.getAs(RAW));
        assertNotNull(row.getAs(DATA));
        assertNotNull(row.getAs(METADATA));
        assertNotNull(row.getAs(TIMESTAMP));
        assertNotNull(row.getAs(KEY));
        assertNotNull(row.getAs(DATA_TYPE));
        assertNotNull(row.getAs(SOURCE));
        assertNotNull(row.getAs(TABLE));
        assertNotNull(row.getAs(OPERATION));
        assertNotNull(row.getAs(TRANSACTION_ID));
        assertNotNull(row.getAs(CONVERTER));

    }

    private JavaRDD<Row> getData(final String path) {
        return spark
                .read()
                .option("wholetext", "true")
                .text(path)
                .withColumn(RAW, col("value"))
                .javaRDD();
    }

}
