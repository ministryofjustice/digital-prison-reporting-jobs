package uk.gov.justice.digital.service.datareconciliation;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;
import static uk.gov.justice.digital.service.datareconciliation.ReconciliationTolerance.equalWithTolerance;

class ReconciliationToleranceTest {

    @Test
    void exactMatchWithNoToleranceShouldGiveTrue() {
        long value1 = 10;
        long value2 = 10;
        long absoluteTolerance = 0;
        double relativeTolerance = 0.0;

        boolean result = equalWithTolerance(value1, value2, absoluteTolerance, relativeTolerance);
        assertTrue(result);
    }

    @Test
    void absoluteToleranceOf1Allows1Difference() {
        long value1 = 100;
        long value2 = 99;
        long absoluteTolerance = 1;
        double relativeTolerance = 0.0;

        boolean result = equalWithTolerance(value1, value2, absoluteTolerance, relativeTolerance);
        assertTrue(result);
    }

    @Test
    void absoluteToleranceOf1DoesNotAllow2Difference() {
        long value1 = 100;
        long value2 = 98;
        long absoluteTolerance = 1;
        double relativeTolerance = 0.0;

        boolean result = equalWithTolerance(value1, value2, absoluteTolerance, relativeTolerance);
        assertFalse(result);
    }

    @Test
    void relativeToleranceOf1PercentAllows1PercentDifference() {
        long value1 = 100;
        long value2 = 99;
        long absoluteTolerance = 0;
        double relativeTolerance = 0.01;

        boolean result = equalWithTolerance(value1, value2, absoluteTolerance, relativeTolerance);
        assertTrue(result);
    }

    @Test
    void relativeToleranceOf1PercentDoesNotAllow2PercentDifference() {
        long value1 = 100;
        long value2 = 98;
        long absoluteTolerance = 0;
        double relativeTolerance = 0.01;

        boolean result = equalWithTolerance(value1, value2, absoluteTolerance, relativeTolerance);
        assertFalse(result);
    }

    @Test
    void matchWithinAbsoluteToleranceShouldGiveTrue() {
        long value1 = 10;
        long value2 = 9;
        long absoluteTolerance = 1;
        double relativeTolerance = 0.0;

        boolean result = equalWithTolerance(value1, value2, absoluteTolerance, relativeTolerance);
        assertTrue(result);
    }

    @Test
    void matchOutsideAbsoluteToleranceShouldGiveFalse() {
        long value1 = 10;
        long value2 = 8;
        long absoluteTolerance = 1;
        double relativeTolerance = 0.0;

        boolean result = equalWithTolerance(value1, value2, absoluteTolerance, relativeTolerance);
        assertFalse(result);
    }

    @Test
    void matchWithinRelativeToleranceShouldGiveTrue() {
        long value1 = 10;
        long value2 = 9;
        long absoluteTolerance = 0;
        double relativeTolerance = 0.1;

        boolean result = equalWithTolerance(value1, value2, absoluteTolerance, relativeTolerance);
        assertTrue(result);
    }

    @Test
    void matchOutsideRelativeToleranceShouldGiveFalse() {
        long value1 = 10;
        long value2 = 8;
        long absoluteTolerance = 0;
        double relativeTolerance = 0.1;

        boolean result = equalWithTolerance(value1, value2, absoluteTolerance, relativeTolerance);
        assertFalse(result);
    }

    @Test
    void matchOutsideRelativeAndAbsoluteTolerancesShouldGiveFalse() {
        long value1 = 10;
        long value2 = 8;
        long absoluteTolerance = 1;
        double relativeTolerance = 0.1;

        boolean result = equalWithTolerance(value1, value2, absoluteTolerance, relativeTolerance);
        assertFalse(result);
    }

    @Test
    void matchWithinRelativeToleranceButOutsideAbsoluteToleranceShouldGiveTrue() {
        long value1 = 10;
        long value2 = 8;
        long absoluteTolerance = 1;
        double relativeTolerance = 0.5;

        boolean result = equalWithTolerance(value1, value2, absoluteTolerance, relativeTolerance);
        assertTrue(result);
    }

    @Test
    void matchWithinAbsoluteToleranceButOutsideRelativeToleranceShouldGiveTrue() {
        long value1 = 100;
        long value2 = 80;
        long absoluteTolerance = 20;
        double relativeTolerance = 0.01;

        boolean result = equalWithTolerance(value1, value2, absoluteTolerance, relativeTolerance);
        assertTrue(result);
    }

    @Test
    void nonExactMatchWithNoToleranceShouldGiveFalse() {
        long value1 = 10;
        long value2 = 9;
        long absoluteTolerance = 0;
        double relativeTolerance = 0.0;

        boolean result = equalWithTolerance(value1, value2, absoluteTolerance, relativeTolerance);
        assertFalse(result);
    }

    @Test
    void exactMatchWithAbsoluteToleranceShouldGiveTrue() {
        long value1 = 10;
        long value2 = 10;
        long absoluteTolerance = 1;
        double relativeTolerance = 0.0;

        boolean result = equalWithTolerance(value1, value2, absoluteTolerance, relativeTolerance);
        assertTrue(result);
    }

    @Test
    void exactMatchWithRelativeToleranceShouldGiveTrue() {
        long value1 = 10;
        long value2 = 10;
        long absoluteTolerance = 0;
        double relativeTolerance = 0.05;

        boolean result = equalWithTolerance(value1, value2, absoluteTolerance, relativeTolerance);
        assertTrue(result);
    }

    @Test
    void exactMatchWithBothTolerancesShouldGiveTrue() {
        long value1 = 10;
        long value2 = 10;
        long absoluteTolerance = 1;
        double relativeTolerance = 0.05;

        boolean result = equalWithTolerance(value1, value2, absoluteTolerance, relativeTolerance);
        assertTrue(result);
    }

    @ParameterizedTest
    @CsvSource({
            "-1,-1",
            "-1,0",
            "-1,1",
            "0,-1",
            "1,-1"
    })
    void shouldThrowForNegativeValues(long value1, long value2) {
        assertThrows(IllegalArgumentException.class, () -> equalWithTolerance(value1, value2, 0L, 0.0));
    }

    @Test
    void shouldThrowForNegativeAbsoluteTolerance() {
        assertThrows(IllegalArgumentException.class, () -> equalWithTolerance(1, 1, -1L, 0.0));
    }

    @Test
    void shouldThrowForNegativeRelativeTolerance() {
        assertThrows(IllegalArgumentException.class, () -> equalWithTolerance(1, 1, 0L, -0.01));
    }

    @Test
    void shouldThrowForRelativeToleranceGreaterThan1() {
        assertThrows(IllegalArgumentException.class, () -> equalWithTolerance(1, 1, 0L, 1.01));
    }

    @Test
    void shouldThrowForNaNRelativeTolerance() {
        assertThrows(IllegalArgumentException.class, () -> equalWithTolerance(1, 1, 0L, Double.NaN));
    }

}