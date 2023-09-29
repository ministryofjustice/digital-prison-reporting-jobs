package uk.gov.justice.digital.converter.dms;

import lombok.val;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.config.BaseSparkTest;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.*;

class DMS_3_4_7_ConverterIntegrationTest extends BaseSparkTest {

    private static final DMS_3_4_7 underTest = new DMS_3_4_7(spark);

    @Test
    void shouldConvertValidDataCorrectly() {
        // Load JSON as text and slurp in the context of the whole file so we can read multiline JSON files.
        val inputDf = getData(DATA_LOAD_RECORD_PATH);

        val converted = underTest.convert(inputDf);

        // Strict schema validation is applied so checking accounts agree should be sufficient here.
        assertEquals(inputDf.count(), converted.count());
    }

    @Test
    void shouldConvertARawDataRecordIntoTheCorrectColumns() {
        val inputDf = getData(DATA_LOAD_RECORD_PATH);

        val converted = underTest.convert(inputDf);
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
        val inputDf = getData(DATA_CONTROL_RECORD_PATH);

        val converted = underTest.convert(inputDf);
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
        val inputDf = getData(DATA_UPDATE_RECORD_PATH);

        val converted = underTest.convert(inputDf);

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
}