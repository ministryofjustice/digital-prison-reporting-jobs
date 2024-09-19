package uk.gov.justice.digital.service.datareconciliation.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CurrentStateTotalCountsTest {

    @Test
    void shouldBeFailureIfAnyResultHasMismatchedCounts() {
        // Exhaustive testing of different cases is included in the tests for CurrentStateTableCount
        CurrentStateTotalCounts underTest = new CurrentStateTotalCounts();
        underTest.put("table1", new CurrentStateTableCount(1L, 1L, 1L, 1L));
        underTest.put("table2", new CurrentStateTableCount(999L, 1L, 1L, 1L));

        assertFalse(underTest.isSuccess());
    }

    @Test
    void shouldBeSuccessIfAllResultsHaveMatchedCounts() {
        CurrentStateTotalCounts underTest = new CurrentStateTotalCounts();
        underTest.put("table1", new CurrentStateTableCount(1L, 1L, 1L, 1L));
        underTest.put("table2", new CurrentStateTableCount(2L, 2L, 2L));

        assertTrue(underTest.isSuccess());
    }

    @Test
    void shouldBeSuccessIfThereAreNoCounts() {
        CurrentStateTotalCounts underTest = new CurrentStateTotalCounts();
        assertTrue(underTest.isSuccess());
    }

    @Test
    void shouldSummariseResultsIfTheyDoNotMatch() {
        CurrentStateTotalCounts underTest = new CurrentStateTotalCounts();
        underTest.put("table1", new CurrentStateTableCount(1L, 1L, 1L, 1L));
        underTest.put("table2", new CurrentStateTableCount(2L, 2L, 1L));

        String expected = "Current State Total Counts DO NOT MATCH:\n" +
                "For table table2:\n" +
                "\tNomis: 2, Structured Zone: 2, Curated Zone: 1, Operational DataStore: skipped\t - MISMATCH\n" +
                "For table table1:\n" +
                "\tNomis: 1, Structured Zone: 1, Curated Zone: 1, Operational DataStore: 1\t - MATCH\n";
        assertEquals(expected, underTest.summary());
    }

    @Test
    void shouldSummariseResultsIfTheyMatch() {
        CurrentStateTotalCounts underTest = new CurrentStateTotalCounts();
        underTest.put("table1", new CurrentStateTableCount(1L, 1L, 1L, 1L));
        underTest.put("table2", new CurrentStateTableCount(2L, 2L, 2L));

        String expected = "Current State Total Counts MATCH:\n" +
                "For table table2:\n" +
                "\tNomis: 2, Structured Zone: 2, Curated Zone: 2, Operational DataStore: skipped\t - MATCH\n" +
                "For table table1:\n" +
                "\tNomis: 1, Structured Zone: 1, Curated Zone: 1, Operational DataStore: 1\t - MATCH\n";
        assertEquals(expected, underTest.summary());
    }

    @Test
    void shouldPutAndGetCurrentStateTableCounts() {
        String tableName = "table1";

        CurrentStateTotalCounts underTest = new CurrentStateTotalCounts();
        assertNull(underTest.get(tableName));

        CurrentStateTableCount currentStateTableCount = new CurrentStateTableCount(1L, 1L, 1L, 1L);
        underTest.put(tableName, currentStateTableCount);

        assertEquals(currentStateTableCount, underTest.get(tableName));
    }

    @Test
    void shouldBeEqualToAnotherWithSameValues() {
        String tableName = "table1";

        CurrentStateTotalCounts underTest1 = new CurrentStateTotalCounts();
        CurrentStateTotalCounts underTest2 = new CurrentStateTotalCounts();

        underTest1.put(tableName, new CurrentStateTableCount(1L, 1L, 1L, 1L));
        underTest2.put(tableName, new CurrentStateTableCount(1L, 1L, 1L, 1L));

        assertEquals(underTest1, underTest2);
    }

    @Test
    void shouldNotBeEqualToAnotherWithDifferentValues() {
        String tableName = "table1";

        CurrentStateTotalCounts underTest1 = new CurrentStateTotalCounts();
        CurrentStateTotalCounts underTest2 = new CurrentStateTotalCounts();

        underTest1.put(tableName, new CurrentStateTableCount(1L, 1L, 1L, 1L));
        underTest2.put(tableName, new CurrentStateTableCount(2L, 1L, 1L, 1L));

        assertNotEquals(underTest1, underTest2);
    }

    @Test
    void shouldNotBeEqualToAnotherWithDifferentTables() {

        CurrentStateTotalCounts underTest1 = new CurrentStateTotalCounts();
        CurrentStateTotalCounts underTest2 = new CurrentStateTotalCounts();

        CurrentStateTableCount count = new CurrentStateTableCount(1L, 1L, 1L, 1L);
        underTest1.put("table1", count);
        underTest2.put("table2", count);

        assertNotEquals(underTest1, underTest2);
    }
}