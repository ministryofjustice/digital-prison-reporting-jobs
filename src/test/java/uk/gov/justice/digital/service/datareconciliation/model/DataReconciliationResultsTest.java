package uk.gov.justice.digital.service.datareconciliation.model;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DataReconciliationResultsTest {

    private static final CurrentStateTotalCounts matchingCurrentStateTotalCounts = new CurrentStateTotalCounts();
    private static final CurrentStateTotalCounts notMatchingCurrentStateTotalCounts = new CurrentStateTotalCounts();
    private static final Map<String, ChangeDataTableCount> changeDataTableCountMap1 = new HashMap<>();
    private static final Map<String, ChangeDataTableCount> changeDataTableCountMap2 = new HashMap<>();


    @BeforeAll
    static void setUpSharedTestData() {
        matchingCurrentStateTotalCounts.put("table1", new CurrentStateTableCount(0.0, 0L, 1L, 1L, 1L));
        notMatchingCurrentStateTotalCounts.put("table1", new CurrentStateTableCount(0.0, 0L, 1L, 2L, 3L));
        changeDataTableCountMap1.put("table1", new ChangeDataTableCount(1L, 1L, 1L));
        changeDataTableCountMap2.put("table1", new ChangeDataTableCount(1L, 2L, 1L));
    }

    @Test
    void isSuccessShouldReturnTrueIfAllCountsMatch() {
        ChangeDataTotalCounts allMatchingChangeDataTotalCounts =
                new ChangeDataTotalCounts(changeDataTableCountMap1, changeDataTableCountMap1, changeDataTableCountMap1);

        DataReconciliationResults underTest =
                new DataReconciliationResults(Arrays.asList(matchingCurrentStateTotalCounts, allMatchingChangeDataTotalCounts));
        assertTrue(underTest.isSuccess());
    }

    @Test
    void isSuccessShouldReturnFalseIfAllCountsDoNotMatch() {
        ChangeDataTotalCounts notMatchingChangeDataTotalCounts =
                new ChangeDataTotalCounts(changeDataTableCountMap1, changeDataTableCountMap2, changeDataTableCountMap1);

        DataReconciliationResults underTest =
                new DataReconciliationResults(Arrays.asList(notMatchingCurrentStateTotalCounts, notMatchingChangeDataTotalCounts));

        assertFalse(underTest.isSuccess());
    }

    @Test
    void isSuccessShouldReturnFalseIfCurrentStateCountsDoNotMatch() {
        ChangeDataTotalCounts matchingChangeDataTotalCounts =
                new ChangeDataTotalCounts(changeDataTableCountMap1, changeDataTableCountMap1, changeDataTableCountMap1);

        DataReconciliationResults underTest =
                new DataReconciliationResults(Arrays.asList(notMatchingCurrentStateTotalCounts, matchingChangeDataTotalCounts));

        assertFalse(underTest.isSuccess());
    }

    @Test
    void isSuccessShouldReturnFalseIfChangeDataCountsDoNotMatch() {
        ChangeDataTotalCounts notMatchingChangeDataTotalCounts =
                new ChangeDataTotalCounts(changeDataTableCountMap1, changeDataTableCountMap2, changeDataTableCountMap1);

        DataReconciliationResults underTest =
                new DataReconciliationResults(Arrays.asList(matchingCurrentStateTotalCounts, notMatchingChangeDataTotalCounts));

        assertFalse(underTest.isSuccess());
    }

    @Test
    void shouldGiveSummary() {
        ChangeDataTotalCounts allMatchingChangeDataTotalCounts =
                new ChangeDataTotalCounts(changeDataTableCountMap1, changeDataTableCountMap1, changeDataTableCountMap1);

        DataReconciliationResults underTest =
                new DataReconciliationResults(Arrays.asList(matchingCurrentStateTotalCounts, allMatchingChangeDataTotalCounts));
        String expected = "\n\nCurrent State Total Counts MATCH:\n" +
                "For table table1:\n" +
                "\tData Source: 1, Structured Zone: 1, Curated Zone: 1, Operational DataStore: skipped\t - MATCH\n" +
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

    @Test
    void shouldCalculateNumReconciliationChecksFailingForMixedSuccessAndFailure() {
        DataReconciliationResult r1 = mock(DataReconciliationResult.class);
        DataReconciliationResult r2 = mock(DataReconciliationResult.class);
        DataReconciliationResult r3 = mock(DataReconciliationResult.class);

        when(r1.isSuccess()).thenReturn(true);
        when(r2.isSuccess()).thenReturn(true);
        when(r3.isSuccess()).thenReturn(false);

        DataReconciliationResults results = new DataReconciliationResults(Arrays.asList(r1, r2, r3));
        assertEquals(1L, results.numReconciliationChecksFailing());
    }

    @Test
    void shouldCalculateNumReconciliationChecksFailingForAllSuccess() {
        DataReconciliationResult r1 = mock(DataReconciliationResult.class);
        DataReconciliationResult r2 = mock(DataReconciliationResult.class);
        DataReconciliationResult r3 = mock(DataReconciliationResult.class);

        when(r1.isSuccess()).thenReturn(true);
        when(r2.isSuccess()).thenReturn(true);
        when(r3.isSuccess()).thenReturn(true);

        DataReconciliationResults results = new DataReconciliationResults(Arrays.asList(r1, r2, r3));
        assertEquals(0L, results.numReconciliationChecksFailing());
    }

    @Test
    void shouldCalculateNumReconciliationChecksFailingForAllFailure() {
        DataReconciliationResult r1 = mock(DataReconciliationResult.class);
        DataReconciliationResult r2 = mock(DataReconciliationResult.class);
        DataReconciliationResult r3 = mock(DataReconciliationResult.class);

        when(r1.isSuccess()).thenReturn(false);
        when(r2.isSuccess()).thenReturn(false);
        when(r3.isSuccess()).thenReturn(false);

        DataReconciliationResults results = new DataReconciliationResults(Arrays.asList(r1, r2, r3));
        assertEquals(3L, results.numReconciliationChecksFailing());
    }
}