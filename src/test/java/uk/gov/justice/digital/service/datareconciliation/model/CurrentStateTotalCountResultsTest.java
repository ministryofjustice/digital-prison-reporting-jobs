package uk.gov.justice.digital.service.datareconciliation.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CurrentStateTotalCountResultsTest {

    @Test
    void shouldGiveFailureIfAnyResultHasMismatchedCounts() {
        CurrentStateTotalCountResults underTest = new CurrentStateTotalCountResults();
        underTest.put("table1", new CurrentStateCountTableResult(1L, 1L, 1L, 1L));
        underTest.put("table2", new CurrentStateCountTableResult(999L, 1L, 1L, 1L));

        assertTrue(underTest.isFailure());
    }

    @Test
    void shouldGiveSuccessIfAllResultsHaveMatchedCounts() {
        CurrentStateTotalCountResults underTest = new CurrentStateTotalCountResults();
        underTest.put("table1", new CurrentStateCountTableResult(1L, 1L, 1L, 1L));
        underTest.put("table2", new CurrentStateCountTableResult(2L, 2L, 2L));

        assertFalse(underTest.isFailure());
    }

    @Test
    void shouldSummariseResults() {
        CurrentStateTotalCountResults underTest = new CurrentStateTotalCountResults();
        underTest.put("table1", new CurrentStateCountTableResult(1L, 1L, 1L, 1L));
        underTest.put("table2", new CurrentStateCountTableResult(2L, 2L, 1L));

        String expected = "Current State Count Results:\n" +
                "For table table2:\n" +
                "MISMATCH: Nomis: 2, Structured Zone: 2, Curated Zone: 1, Operational DataStore: skipped\n" +
                "For table table1:\n" +
                "   MATCH: Nomis: 1, Structured Zone: 1, Curated Zone: 1, Operational DataStore: 1\n";
        assertEquals(expected, underTest.summary());
    }
}