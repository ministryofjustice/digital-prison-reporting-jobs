package uk.gov.justice.digital.service.datareconciliation.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class ChangeDataTotalCountsTest {

    private static final String TABLE_NAME = "table1";

    static Stream<Object> countsThatDoNotMatch() {
        return Stream.of(
                new Object[]{2L, 1L, 1L},
                new Object[]{1L, 2L, 1L},
                new Object[]{1L, 1L, 2L}
        );
    }

    @Test
    void shouldBeSuccessForMatchingCounts() {
        Map<String, ChangeDataTableCount> rawCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedCounts = new HashMap<>();

        rawCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        dmsCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        dmsAppliedCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));

        ChangeDataTotalCounts underTest = new ChangeDataTotalCounts(rawCounts, dmsCounts, dmsAppliedCounts);
        assertTrue(underTest.isSuccess());
    }

    @ParameterizedTest
    @CsvSource({
            "0.0,0,100,100,100",
            "0.01,0,99,100,100",
            "0.01,0,100,99,100",
            "0.01,0,100,100,99",
            "0.01,0,100,99,99",
            "0.01,0,99,100,99",
            "0.01,0,99,99,100",
            "0.0,1,100,100,99",
            "0.0,1,100,99,99",
            "0.0,1,99,100,99",
            "0.0,1,99,99,100",
    })
    void shouldBeSuccessForInsertCountsWithinTolerance(double relativeTolerance, long absoluteTolerance, long rawInsertCount, long dmsInsertCount, long dmsAppliedInsertCount) {
        Map<String, ChangeDataTableCount> rawCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedCounts = new HashMap<>();

        rawCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, rawInsertCount, 1L, 1L));
        dmsCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, dmsInsertCount, 1L, 1L));
        dmsAppliedCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, dmsAppliedInsertCount, 1L, 1L));

        ChangeDataTotalCounts underTest = new ChangeDataTotalCounts(rawCounts, dmsCounts, dmsAppliedCounts);
        assertTrue(underTest.isSuccess());
    }

    @ParameterizedTest
    @CsvSource({
            "0.0,0,100,100,100",
            "0.01,0,99,100,100",
            "0.01,0,100,99,100",
            "0.01,0,100,100,99",
            "0.01,0,100,99,99",
            "0.01,0,99,100,99",
            "0.01,0,99,99,100",
            "0.0,1,100,100,99",
            "0.0,1,100,99,99",
            "0.0,1,99,100,99",
            "0.0,1,99,99,100",
    })
    void shouldBeSuccessForUpdateCountsWithinTolerance(double relativeTolerance, long absoluteTolerance, long rawUpdateCount, long dmsUpdateCount, long dmsAppliedUpdateCount) {
        Map<String, ChangeDataTableCount> rawCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedCounts = new HashMap<>();

        rawCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, rawUpdateCount, 1L));
        dmsCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, dmsUpdateCount, 1L));
        dmsAppliedCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, dmsAppliedUpdateCount, 1L));

        ChangeDataTotalCounts underTest = new ChangeDataTotalCounts(rawCounts, dmsCounts, dmsAppliedCounts);
        assertTrue(underTest.isSuccess());
    }

    @ParameterizedTest
    @CsvSource({
            "0.0,0,100,100,100",
            "0.01,0,99,100,100",
            "0.01,0,100,99,100",
            "0.01,0,100,100,99",
            "0.01,0,100,99,99",
            "0.01,0,99,100,99",
            "0.01,0,99,99,100",
            "0.0,1,100,100,99",
            "0.0,1,100,99,99",
            "0.0,1,99,100,99",
            "0.0,1,99,99,100",
    })
    void shouldBeSuccessForDeleteCountsWithinTolerance(double relativeTolerance, long absoluteTolerance, long rawDeleteCount, long dmsDeleteCount, long dmsAppliedDeleteCount) {
        Map<String, ChangeDataTableCount> rawCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedCounts = new HashMap<>();

        rawCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, 1L, rawDeleteCount));
        dmsCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, 1L, dmsDeleteCount));
        dmsAppliedCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, 1L, dmsAppliedDeleteCount));

        ChangeDataTotalCounts underTest = new ChangeDataTotalCounts(rawCounts, dmsCounts, dmsAppliedCounts);
        assertTrue(underTest.isSuccess());
    }

    @ParameterizedTest
    @CsvSource({
            "0.0,0,99,100,100",
            "0.0,0,100,99,100",
            "0.0,0,100,100,99",
            "0.01,0,98,100,100",
            "0.01,0,100,98,100",
            "0.01,0,100,100,98",
            "0.01,0,100,98,98",
            "0.01,0,98,100,98",
            "0.01,0,98,98,100",
            "0.0,1,100,100,98",
            "0.0,1,100,98,98",
            "0.0,1,98,100,98",
            "0.0,1,98,98,100",
    })
    void shouldBeFailureForInsertCountsOutsideTolerance(double relativeTolerance, long absoluteTolerance, long rawInsertCount, long dmsInsertCount, long dmsAppliedInsertCount) {
        Map<String, ChangeDataTableCount> rawCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedCounts = new HashMap<>();

        rawCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, rawInsertCount, 1L, 1L));
        dmsCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, dmsInsertCount, 1L, 1L));
        dmsAppliedCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, dmsAppliedInsertCount, 1L, 1L));

        ChangeDataTotalCounts underTest = new ChangeDataTotalCounts(rawCounts, dmsCounts, dmsAppliedCounts);
        assertFalse(underTest.isSuccess());
    }

    @ParameterizedTest
    @CsvSource({
            "0.0,0,99,100,100",
            "0.0,0,100,99,100",
            "0.0,0,100,100,99",
            "0.01,0,98,100,100",
            "0.01,0,100,98,100",
            "0.01,0,100,100,98",
            "0.01,0,100,98,98",
            "0.01,0,98,100,98",
            "0.01,0,98,98,100",
            "0.0,1,100,100,98",
            "0.0,1,100,98,98",
            "0.0,1,98,100,98",
            "0.0,1,98,98,100",
    })
    void shouldBeFailureForUpdateCountsOutsideTolerance(double relativeTolerance, long absoluteTolerance, long rawUpdateCount, long dmsUpdateCount, long dmsAppliedUpdateCount) {
        Map<String, ChangeDataTableCount> rawCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedCounts = new HashMap<>();

        rawCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, rawUpdateCount, 1L));
        dmsCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, dmsUpdateCount, 1L));
        dmsAppliedCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, dmsAppliedUpdateCount, 1L));

        ChangeDataTotalCounts underTest = new ChangeDataTotalCounts(rawCounts, dmsCounts, dmsAppliedCounts);
        assertFalse(underTest.isSuccess());
    }

    @ParameterizedTest
    @CsvSource({
            "0.0,0,99,100,100",
            "0.0,0,100,99,100",
            "0.0,0,100,100,99",
            "0.01,0,98,100,100",
            "0.01,0,100,98,100",
            "0.01,0,100,100,98",
            "0.01,0,100,98,98",
            "0.01,0,98,100,98",
            "0.01,0,98,98,100",
            "0.0,1,100,100,98",
            "0.0,1,100,98,98",
            "0.0,1,98,100,98",
            "0.0,1,98,98,100",
    })
    void shouldBeFailureForDeleteCountsOutsideTolerance(double relativeTolerance, long absoluteTolerance, long rawDeleteCount, long dmsDeleteCount, long dmsAppliedDeleteCount) {
        Map<String, ChangeDataTableCount> rawCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedCounts = new HashMap<>();

        rawCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, 1L, rawDeleteCount));
        dmsCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, 1L, dmsDeleteCount));
        dmsAppliedCounts.put(TABLE_NAME, new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, 1L, dmsAppliedDeleteCount));

        ChangeDataTotalCounts underTest = new ChangeDataTotalCounts(rawCounts, dmsCounts, dmsAppliedCounts);
        assertFalse(underTest.isSuccess());
    }

    @ParameterizedTest
    @MethodSource("countsThatDoNotMatch")
    void shouldBeFailureForDifferentInsertCountsWithZeroTolerance(long rawInsertCount, long dmsInsertCount, long dmsAppliedInsertCount) {
        Map<String, ChangeDataTableCount> rawCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedCounts = new HashMap<>();

        rawCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, rawInsertCount, 1L, 1L));
        dmsCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, dmsInsertCount, 1L, 1L));
        dmsAppliedCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, dmsAppliedInsertCount, 1L, 1L));

        ChangeDataTotalCounts underTest = new ChangeDataTotalCounts(rawCounts, dmsCounts, dmsAppliedCounts);
        assertFalse(underTest.isSuccess());
    }

    @ParameterizedTest
    @MethodSource("countsThatDoNotMatch")
    void shouldBeFailureForDifferentUpdateCountsWithZeroTolerance(long rawUpdateCount, long dmsUpdateCount, long dmsAppliedUpdateCount) {
        Map<String, ChangeDataTableCount> rawCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedCounts = new HashMap<>();

        rawCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, rawUpdateCount, 1L));
        dmsCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, dmsUpdateCount, 1L));
        dmsAppliedCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, dmsAppliedUpdateCount, 1L));

        ChangeDataTotalCounts underTest = new ChangeDataTotalCounts(rawCounts, dmsCounts, dmsAppliedCounts);
        assertFalse(underTest.isSuccess());
    }

    @ParameterizedTest
    @MethodSource("countsThatDoNotMatch")
    void shouldBeFailureForDifferentDeleteCountsWithZeroTolerance(long rawDeleteCount, long dmsDeleteCount, long dmsAppliedDeleteCount) {
        Map<String, ChangeDataTableCount> rawCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedCounts = new HashMap<>();

        rawCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, rawDeleteCount));
        dmsCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, dmsDeleteCount));
        dmsAppliedCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, dmsAppliedDeleteCount));

        ChangeDataTotalCounts underTest = new ChangeDataTotalCounts(rawCounts, dmsCounts, dmsAppliedCounts);
        assertFalse(underTest.isSuccess());
    }

    @Test
    void shouldBeFailureForDifferentDmsTable() {
        Map<String, ChangeDataTableCount> rawCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedCounts = new HashMap<>();

        rawCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        dmsCounts.put("different table", new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        dmsAppliedCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));

        ChangeDataTotalCounts underTest = new ChangeDataTotalCounts(rawCounts, dmsCounts, dmsAppliedCounts);
        assertFalse(underTest.isSuccess());
    }

    @Test
    void shouldBeFailureForDifferentDmsAppliedTable() {
        Map<String, ChangeDataTableCount> rawCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedCounts = new HashMap<>();

        rawCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        dmsCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        dmsAppliedCounts.put("different table", new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));

        ChangeDataTotalCounts underTest = new ChangeDataTotalCounts(rawCounts, dmsCounts, dmsAppliedCounts);
        assertFalse(underTest.isSuccess());
    }

    @Test
    void shouldGiveSummaryWhenAllMatch() {
        Map<String, ChangeDataTableCount> rawCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedCounts = new HashMap<>();

        rawCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        dmsCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        dmsAppliedCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));

        ChangeDataTotalCounts underTest = new ChangeDataTotalCounts(rawCounts, dmsCounts, dmsAppliedCounts);

        String expected = "Change Data Total Counts MATCH:\n" +
                "\n" +
                "For table table1 MATCH:\n" +
                "\tInserts: 1, Updates: 1, Deletes: 1\t - Raw\n" +
                "\tInserts: 1, Updates: 1, Deletes: 1\t - DMS\n" +
                "\tInserts: 1, Updates: 1, Deletes: 1\t - DMS Applied\n";
        String actual = underTest.summary();
        assertEquals(expected, actual);
    }

    @Test
    void shouldGiveSummaryWhenMatchWithinTolerance() {
        Map<String, ChangeDataTableCount> rawCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedCounts = new HashMap<>();

        rawCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 1L, 1L, 1L, 1L));
        dmsCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 1L, 2L, 1L, 1L));
        dmsAppliedCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));

        ChangeDataTotalCounts underTest = new ChangeDataTotalCounts(rawCounts, dmsCounts, dmsAppliedCounts);

        String expected = "Change Data Total Counts MATCH (within tolerance):\n" +
                "\n" +
                "For table table1 MATCH (within tolerance):\n" +
                "\tInserts: 1, Updates: 1, Deletes: 1\t - Raw\n" +
                "\tInserts: 2, Updates: 1, Deletes: 1\t - DMS\n" +
                "\tInserts: 1, Updates: 1, Deletes: 1\t - DMS Applied\n";
        String actual = underTest.summary();
        assertEquals(expected, actual);
    }

    @Test
    void shouldGiveSummaryWhenDoNotMatch() {
        Map<String, ChangeDataTableCount> rawCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedCounts = new HashMap<>();

        rawCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        dmsCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 2L, 1L));
        dmsAppliedCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, 3L));

        ChangeDataTotalCounts underTest = new ChangeDataTotalCounts(rawCounts, dmsCounts, dmsAppliedCounts);

        String expected = "Change Data Total Counts DO NOT MATCH:\n" +
                "\n" +
                "For table table1 DOES NOT MATCH:\n" +
                "\tInserts: 1, Updates: 1, Deletes: 1\t - Raw\n" +
                "\tInserts: 1, Updates: 2, Deletes: 1\t - DMS\n" +
                "\tInserts: 1, Updates: 1, Deletes: 3\t - DMS Applied\n";
        String actual = underTest.summary();
        assertEquals(expected, actual);
    }

    @Test
    void shouldGiveSummaryWhenRawHasDifferentTables() {
        Map<String, ChangeDataTableCount> rawCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedCounts = new HashMap<>();

        rawCounts.put("different table", new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        dmsCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        dmsAppliedCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));

        ChangeDataTotalCounts underTest = new ChangeDataTotalCounts(rawCounts, dmsCounts, dmsAppliedCounts);

        String expected = "Change Data Total Counts DO NOT MATCH:\n" +
                "\n" +
                "\n" +
                "The set of tables for DMS vs Raw zone DO NOT MATCH\n" +
                "\n" +
                "DMS Tables missing in Raw: [table1]\n" +
                "Raw Zone/Raw Archive Tables missing in DMS: [different table]\n" +
                "For table table1 DOES NOT MATCH:\n" +
                "\tMISSING COUNTS\t - Raw\n" +
                "\tInserts: 1, Updates: 1, Deletes: 1\t - DMS\n" +
                "\tInserts: 1, Updates: 1, Deletes: 1\t - DMS Applied\n";
        String actual = underTest.summary();
        assertEquals(expected, actual);
    }

    @Test
    void shouldGiveSummaryWhenDmsAppliedHasDifferentTables() {
        Map<String, ChangeDataTableCount> rawCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedCounts = new HashMap<>();

        rawCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        rawCounts.put("table2", new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        dmsCounts.put(TABLE_NAME, new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        dmsAppliedCounts.put("different table", new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));

        ChangeDataTotalCounts underTest = new ChangeDataTotalCounts(rawCounts, dmsCounts, dmsAppliedCounts);

        String expected = "Change Data Total Counts DO NOT MATCH:\n" +
                "\n" +
                "\n" +
                "The set of tables for DMS vs Raw zone DO NOT MATCH\n" +
                "\n" +
                "DMS Tables missing in Raw: []\n" +
                "Raw Zone/Raw Archive Tables missing in DMS: [table2]\n" +
                "For table table1 DOES NOT MATCH:\n" +
                "\tInserts: 1, Updates: 1, Deletes: 1\t - Raw\n" +
                "\tInserts: 1, Updates: 1, Deletes: 1\t - DMS\n" +
                "\tMISSING COUNTS\t - DMS Applied\n";
        String actual = underTest.summary();
        assertEquals(expected, actual);
    }
}