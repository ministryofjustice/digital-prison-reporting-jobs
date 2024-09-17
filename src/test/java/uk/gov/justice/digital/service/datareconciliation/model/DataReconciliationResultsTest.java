package uk.gov.justice.digital.service.datareconciliation.model;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataReconciliationResultsTest {

    private static final CurrentStateTotalCounts matchingCurrentStateTotalCounts = new CurrentStateTotalCounts();
    private static final CurrentStateTotalCounts notMatchingCurrentStateTotalCounts = new CurrentStateTotalCounts();
    private static final Map<String, ChangeDataTableCount> changeDataTableCountMap1 = new HashMap<>();
    private static final Map<String, ChangeDataTableCount> changeDataTableCountMap2 = new HashMap<>();


    @BeforeAll
    static void setUpSharedTestData() {
        matchingCurrentStateTotalCounts.put("table1", new CurrentStateTableCount(1L, 1L, 1L));
        notMatchingCurrentStateTotalCounts.put("table1", new CurrentStateTableCount(1L, 2L, 3L));
        changeDataTableCountMap1.put("table1", new ChangeDataTableCount(1L, 1L, 1L));
        changeDataTableCountMap2.put("table1", new ChangeDataTableCount(1L, 2L, 1L));
    }

    @Test
    void isSuccessShouldReturnTrueIfAllCountsMatch() {
        ChangeDataTotalCounts allMatchingChangeDataTotalCounts =
                new ChangeDataTotalCounts(changeDataTableCountMap1, changeDataTableCountMap1, changeDataTableCountMap1);

        DataReconciliationResults underTest = new DataReconciliationResults(matchingCurrentStateTotalCounts, allMatchingChangeDataTotalCounts);
        assertTrue(underTest.isSuccess());
    }

    @Test
    void isSuccessShouldReturnFalseIfAllCountsDoNotMatch() {
        ChangeDataTotalCounts notMatchingChangeDataTotalCounts =
                new ChangeDataTotalCounts(changeDataTableCountMap1, changeDataTableCountMap2, changeDataTableCountMap1);

        DataReconciliationResults underTest = new DataReconciliationResults(notMatchingCurrentStateTotalCounts, notMatchingChangeDataTotalCounts);

        assertFalse(underTest.isSuccess());
    }

    @Test
    void isSuccessShouldReturnFalseIfCurrentStateCountsDoNotMatch() {
        ChangeDataTotalCounts matchingChangeDataTotalCounts =
                new ChangeDataTotalCounts(changeDataTableCountMap1, changeDataTableCountMap1, changeDataTableCountMap1);

        DataReconciliationResults underTest = new DataReconciliationResults(notMatchingCurrentStateTotalCounts, matchingChangeDataTotalCounts);

        assertFalse(underTest.isSuccess());
    }

    @Test
    void isSuccessShouldReturnFalseIfChangeDataCountsDoNotMatch() {
        ChangeDataTotalCounts notMatchingChangeDataTotalCounts =
                new ChangeDataTotalCounts(changeDataTableCountMap1, changeDataTableCountMap2, changeDataTableCountMap1);

        DataReconciliationResults underTest = new DataReconciliationResults(matchingCurrentStateTotalCounts, notMatchingChangeDataTotalCounts);

        assertFalse(underTest.isSuccess());
    }

    @Test
    void shouldGiveSummary() {
        ChangeDataTotalCounts allMatchingChangeDataTotalCounts =
                new ChangeDataTotalCounts(changeDataTableCountMap1, changeDataTableCountMap1, changeDataTableCountMap1);

        DataReconciliationResults underTest = new DataReconciliationResults(matchingCurrentStateTotalCounts, allMatchingChangeDataTotalCounts);
        String expected = "Current State Total Counts MATCH:\n" +
                "For table table1:\n" +
                "\tNomis: 1, Structured Zone: 1, Curated Zone: 1, Operational DataStore: skipped\t - MATCH\n" +
                "\n" +
                "\n" +
                "Change Data Total Counts MATCH:\n" +
                "\n" +
                "For table table1 MATCH:\n" +
                "\tInserts: 1, Updates: 1, Deletes: 1\t - Raw\n" +
                "\tInserts: 1, Updates: 1, Deletes: 1\t - DMS\n" +
                "\tInserts: 1, Updates: 1, Deletes: 1\t - DMS Applied\n";
        assertEquals(expected, underTest.summary());
    }
}