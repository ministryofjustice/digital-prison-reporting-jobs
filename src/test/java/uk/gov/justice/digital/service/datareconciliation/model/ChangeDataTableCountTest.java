package uk.gov.justice.digital.service.datareconciliation.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChangeDataTableCountTest {

    static Stream<Object> countsThatDoNotMatch() {
        return Stream.of(
                new Object[]{1L, 1L, 1L, 1L, 1L, 2L},
                new Object[]{1L, 1L, 1L, 1L, 2L, 1L},
                new Object[]{1L, 1L, 1L, 2L, 1L, 1L},
                new Object[]{1L, 1L, 2L, 1L, 1L, 1L},
                new Object[]{1L, 2L, 1L, 1L, 1L, 1L},
                new Object[]{2L, 1L, 1L, 1L, 1L, 1L},
                new Object[]{1L, 2L, 2L, 1L, 1L, 1L},
                new Object[]{2L, 2L, 2L, 1L, 1L, 1L}
        );
    }

    @Test
    void countsEqualShouldBeEqualIfCountsMatch() {
        ChangeDataTableCount underTest1 = new ChangeDataTableCount(0.0, 0L, 1L, 2L, 3L);
        ChangeDataTableCount underTest2 = new ChangeDataTableCount(0.0, 0L, 1L, 2L, 3L);

        assertTrue(underTest1.countsEqual(underTest2));
    }

    @Test
    void countsEqualShouldNotBeEqualIfOtherIsNull() {
        ChangeDataTableCount underTest1 = new ChangeDataTableCount(0.0, 0L, 1L, 2L, 3L);

        assertFalse(underTest1.countsEqual(null));
    }

    @Test
    void countsEqualShouldIgnoreDifferencesInTolerancesWhenCalculatingEquality() {
        ChangeDataTableCount underTest1 = new ChangeDataTableCount(0.1, 1L, 1L, 2L, 3L);
        ChangeDataTableCount underTest2 = new ChangeDataTableCount(0.2, 2L, 1L, 2L, 3L);

        assertTrue(underTest1.countsEqual(underTest2));
    }

    @ParameterizedTest
    @MethodSource("countsThatDoNotMatch")
    void countsEqualShouldNotBeEqualIfCountsDoNotMatch(
            long insertCount1, long updateCount1, long deleteCount1, long insertCount2, long updateCount2, long deleteCount2
    ) {
        ChangeDataTableCount underTest1 = new ChangeDataTableCount(0.0, 0L, insertCount1, updateCount1, deleteCount1);
        ChangeDataTableCount underTest2 = new ChangeDataTableCount(0.0, 0L, insertCount2, updateCount2, deleteCount2);
        assertFalse(underTest1.countsEqual(underTest2));
    }

    @Test
    void countsEqualWithinToleranceShouldBeEqualIfCountsMatchExactly() {
        ChangeDataTableCount underTest1 = new ChangeDataTableCount(0.0, 0L, 1L, 2L, 3L);
        ChangeDataTableCount underTest2 = new ChangeDataTableCount(0.0, 0L, 1L, 2L, 3L);

        assertTrue(underTest1.countsEqualWithinTolerance(underTest2));
    }

    @Test
    void countsEqualWithinToleranceShouldNotBeEqualIfOtherIsNull() {
        ChangeDataTableCount underTest1 = new ChangeDataTableCount(0.0, 0L, 1L, 2L, 3L);

        assertFalse(underTest1.countsEqualWithinTolerance(null));
    }

    @ParameterizedTest
    @CsvSource({
            "0.0,0,100,100",
            "0.0,1,100,100",
            "0.1,0,100,100",
            "0.1,1,100,100",
            "0.0,1,100,99",
            "0.01,0,100,99",
            "0.01,1,100,99",
            "0.0,1,99,100",
            "0.01,0,99,100",
            "0.01,1,99,100",
            "0.10,1,90,100",
            "0.01,10,90,100",
    })
    void countsEqualWithinToleranceShouldBeEqualIfInsertCountsAreWithinTolerance(double relativeTolerance, long absoluteTolerance, long insertCount1, long insertCount2) {
        ChangeDataTableCount underTest1 = new ChangeDataTableCount(relativeTolerance, absoluteTolerance, insertCount1, 2L, 3L);
        ChangeDataTableCount underTest2 = new ChangeDataTableCount(relativeTolerance, absoluteTolerance, insertCount2, 2L, 3L);

        assertTrue(underTest1.countsEqualWithinTolerance(underTest2));
    }

    @ParameterizedTest
    @CsvSource({
            "0.0,0,100,100",
            "0.0,1,100,100",
            "0.1,0,100,100",
            "0.1,1,100,100",
            "0.0,1,100,99",
            "0.01,0,100,99",
            "0.01,1,100,99",
            "0.0,1,99,100",
            "0.01,0,99,100",
            "0.01,1,99,100",
            "0.10,1,90,100",
            "0.01,10,90,100",
    })
    void countsEqualWithinToleranceShouldBeEqualIfUpdateCountsAreWithinTolerance(double relativeTolerance, long absoluteTolerance, long updateCount1, long updateCount2) {
        ChangeDataTableCount underTest1 = new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, updateCount1, 3L);
        ChangeDataTableCount underTest2 = new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, updateCount2, 3L);

        assertTrue(underTest1.countsEqualWithinTolerance(underTest2));
    }

    @ParameterizedTest
    @CsvSource({
            "0.0,0,100,100",
            "0.0,1,100,100",
            "0.1,0,100,100",
            "0.1,1,100,100",
            "0.0,1,100,99",
            "0.01,0,100,99",
            "0.01,1,100,99",
            "0.0,1,99,100",
            "0.01,0,99,100",
            "0.01,1,99,100",
            "0.10,1,90,100",
            "0.01,10,90,100",
    })
    void countsEqualWithinToleranceShouldBeEqualIfDeleteCountsAreWithinTolerance(double relativeTolerance, long absoluteTolerance, long deleteCount1, long deleteCount2) {
        ChangeDataTableCount underTest1 = new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, 2L, deleteCount1);
        ChangeDataTableCount underTest2 = new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, 2L, deleteCount2);

        assertTrue(underTest1.countsEqualWithinTolerance(underTest2));
    }

    @ParameterizedTest
    @CsvSource({
            "0.0,0,100,99",
            "0.0,0,99,100",
            "0.0,1,100,98",
            "0.0,1,98,100",
            "0.01,0,98,100",
            "0.01,0,100,98",
    })
    void countsEqualWithinToleranceShouldNotBeEqualIfInsertCountsAreOutsideTolerance(double relativeTolerance, long absoluteTolerance, long insertCount1, long insertCount2) {
        ChangeDataTableCount underTest1 = new ChangeDataTableCount(relativeTolerance, absoluteTolerance, insertCount1, 2L, 3L);
        ChangeDataTableCount underTest2 = new ChangeDataTableCount(relativeTolerance, absoluteTolerance, insertCount2, 2L, 3L);

        assertFalse(underTest1.countsEqualWithinTolerance(underTest2));
    }

    @ParameterizedTest
    @CsvSource({
            "0.0,0,100,99",
            "0.0,0,99,100",
            "0.0,1,100,98",
            "0.0,1,98,100",
            "0.01,0,98,100",
            "0.01,0,100,98",
    })
    void countsEqualWithinToleranceShouldNotBeEqualIfUpdateCountsAreOutsideTolerance(double relativeTolerance, long absoluteTolerance, long updateCount1, long updateCount2) {
        ChangeDataTableCount underTest1 = new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, updateCount1, 3L);
        ChangeDataTableCount underTest2 = new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, updateCount2, 3L);

        assertFalse(underTest1.countsEqualWithinTolerance(underTest2));
    }

    @ParameterizedTest
    @CsvSource({
            "0.0,0,100,99",
            "0.0,0,99,100",
            "0.0,1,100,98",
            "0.0,1,98,100",
            "0.01,0,98,100",
            "0.01,0,100,98",
    })
    void countsEqualWithinToleranceShouldNotBeEqualIfDeleteCountsAreOutsideTolerance(double relativeTolerance, long absoluteTolerance, long deleteCount1, long deleteCount2) {
        ChangeDataTableCount underTest1 = new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, 2L, deleteCount1);
        ChangeDataTableCount underTest2 = new ChangeDataTableCount(relativeTolerance, absoluteTolerance, 1L, 2L, deleteCount2);

        assertFalse(underTest1.countsEqualWithinTolerance(underTest2));
    }

    @Test
    void countsEqualWithinToleranceShouldIgnoreDifferencesInTolerances() {
        ChangeDataTableCount underTest1 = new ChangeDataTableCount(0.1, 1L, 1L, 2L, 3L);
        ChangeDataTableCount underTest2 = new ChangeDataTableCount(0.2, 2L, 1L, 2L, 3L);

        assertTrue(underTest1.countsEqualWithinTolerance(underTest2));
    }

    @Test
    void shouldBeEqualIfCountsMatch() {
        ChangeDataTableCount underTest1 = new ChangeDataTableCount(0.0, 0L, 1L, 2L, 3L);
        ChangeDataTableCount underTest2 = new ChangeDataTableCount(0.0, 0L, 1L, 2L, 3L);

        assertEquals(underTest1, underTest2);
    }

    @ParameterizedTest
    @MethodSource("countsThatDoNotMatch")
    void shouldNotBeEqualIfCountsDoNotMatch(
            long insertCount1, long updateCount1, long deleteCount1, long insertCount2, long updateCount2, long deleteCount2
    ) {
        ChangeDataTableCount underTest1 = new ChangeDataTableCount(0.0, 0L, insertCount1, updateCount1, deleteCount1);
        ChangeDataTableCount underTest2 = new ChangeDataTableCount(0.0, 0L, insertCount2, updateCount2, deleteCount2);
        assertNotEquals(underTest1, underTest2);
    }

    @Test
    void shouldDefaultToZeroCounts() {
        ChangeDataTableCount underTest = new ChangeDataTableCount(0.0, 0L);
        assertEquals(0L, underTest.getInsertCount());
        assertEquals(0L, underTest.getUpdateCount());
        assertEquals(0L, underTest.getDeleteCount());
    }

    @Test
    void toStringShouldBeReadable() {
        ChangeDataTableCount underTest = new ChangeDataTableCount(0.0, 0L);
        underTest.setInsertCount(1L);
        underTest.setUpdateCount(2L);
        underTest.setDeleteCount(3L);
        String expected = "Inserts: 1, Updates: 2, Deletes: 3";
        assertEquals(expected, underTest.toString());
    }

    @Test
    void shouldCombineCounts() {
        ChangeDataTableCount underTest1 = new ChangeDataTableCount(0.0, 0L, 1L, 2L, 3L);
        ChangeDataTableCount underTest2 = new ChangeDataTableCount(0.0, 0L, 2L, 3L, 4L);

        ChangeDataTableCount combined = underTest1.combineCounts(underTest2);

        assertEquals(3L, combined.getInsertCount());
        assertEquals(5L, combined.getUpdateCount());
        assertEquals(7L, combined.getDeleteCount());
    }

    @ParameterizedTest
    @CsvSource({
            "0.01,0.0,0,0",
            "0.0,0.01,0,0",
            "0.0,0.0,1,0",
            "0.0,0.0,0,1",
    })
    void combineCountsShouldThrowIfTolerancesDoNotMatch(double relativeTolerance1, double relativeTolerance2, long absoluteTolerance1, long absoluteTolerance2) {
        ChangeDataTableCount underTest1 = new ChangeDataTableCount(relativeTolerance1, absoluteTolerance1, 1L, 2L, 3L);
        ChangeDataTableCount underTest2 = new ChangeDataTableCount(relativeTolerance2, absoluteTolerance2, 2L, 3L, 4L);

        assertThrows(IllegalArgumentException.class, () -> underTest1.combineCounts(underTest2));
    }

}