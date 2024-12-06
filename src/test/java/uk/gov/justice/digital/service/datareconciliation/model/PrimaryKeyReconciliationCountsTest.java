package uk.gov.justice.digital.service.datareconciliation.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrimaryKeyReconciliationCountsTest {

    @Test
    void isSuccessWhenAllHaveZeroCounts() {
        PrimaryKeyReconciliationCounts underTest = new PrimaryKeyReconciliationCounts();
        underTest.put("source.table1", new PrimaryKeyReconciliationCount(0L, 0L));
        underTest.put("source.table2", new PrimaryKeyReconciliationCount(0L, 0L));
        underTest.put("source.table3", new PrimaryKeyReconciliationCount(0L, 0L));
        assertTrue(underTest.isSuccess());
    }

    @Test
    void isNotSuccessForEmptyCounts() {
        PrimaryKeyReconciliationCounts underTest = new PrimaryKeyReconciliationCounts();
        assertFalse(underTest.isSuccess());
    }

    @Test
    void isNotSuccessWhenAtLeastOneHasNonZeroCounts() {
        PrimaryKeyReconciliationCounts underTest = new PrimaryKeyReconciliationCounts();
        underTest.put("source.table1", new PrimaryKeyReconciliationCount(0L, 1L));
        underTest.put("source.table2", new PrimaryKeyReconciliationCount(0L, 0L));
        underTest.put("source.table3", new PrimaryKeyReconciliationCount(0L, 0L));
        assertFalse(underTest.isSuccess());
    }

    @Test
    void summaryShouldCreateStringWhenNotMatch() {
        PrimaryKeyReconciliationCounts underTest = new PrimaryKeyReconciliationCounts();
        underTest.put("source.table1", new PrimaryKeyReconciliationCount(0L, 1L));
        underTest.put("source.table2", new PrimaryKeyReconciliationCount(0L, 0L));
        underTest.put("source.table3", new PrimaryKeyReconciliationCount(0L, 0L));

        String result = underTest.summary();
        String expected = "Primary Key Reconciliation Counts DO NOT MATCH:\n" +
                "For table source.table1:\n" +
                "\tIn Curated but not Data Source: 0, In Data Source but not Curated: 1\n" +
                "For table source.table2:\n" +
                "\tIn Curated but not Data Source: 0, In Data Source but not Curated: 0\n" +
                "For table source.table3:\n" +
                "\tIn Curated but not Data Source: 0, In Data Source but not Curated: 0\n";

        assertEquals(expected, result);
    }

    @Test
    void summaryShouldCreateStringWhenAllMatch() {
        PrimaryKeyReconciliationCounts underTest = new PrimaryKeyReconciliationCounts();
        underTest.put("source.table1", new PrimaryKeyReconciliationCount(0L, 0L));
        underTest.put("source.table2", new PrimaryKeyReconciliationCount(0L, 0L));
        underTest.put("source.table3", new PrimaryKeyReconciliationCount(0L, 0L));

        String result = underTest.summary();
        String expected = "Primary Key Reconciliation Counts MATCH:\n" +
                "For table source.table1:\n" +
                "\tIn Curated but not Data Source: 0, In Data Source but not Curated: 0\n" +
                "For table source.table2:\n" +
                "\tIn Curated but not Data Source: 0, In Data Source but not Curated: 0\n" +
                "For table source.table3:\n" +
                "\tIn Curated but not Data Source: 0, In Data Source but not Curated: 0\n";

        assertEquals(expected, result);
    }
}