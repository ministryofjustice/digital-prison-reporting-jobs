package uk.gov.justice.digital.service.datareconciliation.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrimaryKeyReconciliationCountTest {

    @Test
    void shouldConvertToString() {
        String expected = "In Curated but not Data Source: 2, In Data Source but not Curated: 5";
        assertEquals(expected, new PrimaryKeyReconciliationCount(2L, 5L).toString());
    }

    @Test
    void countsAreZeroShouldBeTrueForAllZeroCounts() {
        assertTrue(new PrimaryKeyReconciliationCount(0L, 0L).countsAreZero());
    }

    @Test
    void countsAreZeroShouldBeFalseForAllNonZeroCounts() {
        assertFalse(new PrimaryKeyReconciliationCount(2L, 5L).countsAreZero());
    }

    @Test
    void countsAreZeroShouldBeFalseForNonZeroCountInCurated() {
        assertFalse(new PrimaryKeyReconciliationCount(2L, 0L).countsAreZero());
    }

    @Test
    void countsAreZeroShouldBeFalseForNonZeroCountInDataSource() {
        assertFalse(new PrimaryKeyReconciliationCount(0L, 5L).countsAreZero());
    }
}