package uk.gov.justice.digital.service.datareconciliation.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class CurrentStateTableCountTest {

    static Stream<Object> countsThatDoNotMatch() {
        return Stream.of(
                new Object[]{1L, 1L, 1L, 2L},
                new Object[]{1L, 1L, 2L, 1L},
                new Object[]{1L, 2L, 1L, 1L},
                new Object[]{2L, 1L, 1L, 1L},
                new Object[]{1L, 2L, 3L, 1L},
                new Object[]{1L, 2L, 3L, 4L}
        );
    }

    static Stream<Object> countsThatDoNotMatchDisabledOds() {
        return Stream.of(
                new Object[]{1L, 1L, 2L},
                new Object[]{1L, 2L, 1L},
                new Object[]{2L, 1L, 1L},
                new Object[]{1L, 2L, 3L}
        );
    }

    @Test
    void shouldGiveMatchForCountsThatMatch() {
        CurrentStateTableCount underTest = new CurrentStateTableCount(0.0, 0L, 1L, 1L, 1L, 1L);
        assertTrue(underTest.countsMatch());
    }

    @ParameterizedTest
    @CsvSource({
            "1,1,1,0",
            "1,1,0,1",
            "1,0,1,1",
            "0,1,1,1",
            "1,1,0,0",
            "1,0,1,0",
            "1,0,0,1",
            "1,0,0,0",
            "0,1,0,0",
            "0,0,1,0",
            "0,0,0,1",
    })
    void shouldGiveMatchForCountsThatAreWithinAbsoluteTolerance(
            long dataSourceCount,
            long structuredCount,
            long curatedCount,
            long odsCount
    ) {
        CurrentStateTableCount underTest = new CurrentStateTableCount(0.0, 1L, dataSourceCount, structuredCount, curatedCount, odsCount);
        assertTrue(underTest.countsMatch());
    }

    @ParameterizedTest
    @CsvSource({
            "2,2,2,0",
            "2,2,0,2",
            "2,0,2,2",
            "0,2,2,2",
            "2,2,0,0",
            "2,0,2,0",
            "2,0,0,2",
            "2,0,0,0",
            "0,2,0,0",
            "0,0,2,0",
            "0,0,0,2",
    })
    void shouldGiveMisMatchForCountsThatAreOutsideAbsoluteTolerance(
            long dataSourceCount,
            long structuredCount,
            long curatedCount,
            long odsCount
    ) {
        CurrentStateTableCount underTest = new CurrentStateTableCount(0.0, 1L, dataSourceCount, structuredCount, curatedCount, odsCount);
        assertFalse(underTest.countsMatch());
    }

    @ParameterizedTest
    @CsvSource({
            "100,100,100,99",
            "100,100,99,100",
            "100,99,100,100",
            "99,100,100,100",
            "100,100,99,99",
            "100,99,100,99",
            "100,99,99,100",
            "100,99,99,99",
            "99,100,99,99",
            "99,99,100,99",
            "99,99,99,100",
    })
    void shouldGiveMatchForCountsThatAreWithinRelativeTolerance(
            long dataSourceCount,
            long structuredCount,
            long curatedCount,
            long odsCount
    ) {
        CurrentStateTableCount underTest = new CurrentStateTableCount(0.01, 0L, dataSourceCount, structuredCount, curatedCount, odsCount);
        assertTrue(underTest.countsMatch());
    }

    @ParameterizedTest
    @CsvSource({
            "100,100,100,98",
            "100,100,98,100",
            "100,98,100,100",
            "98,100,100,100",
            "100,100,98,98",
            "100,98,100,98",
            "100,98,98,100",
            "100,98,98,98",
            "98,100,98,98",
            "98,98,100,98",
            "98,98,98,100",
    })
    void shouldGiveMisMatchForCountsThatAreOutsideRelativeTolerance(
            long dataSourceCount,
            long structuredCount,
            long curatedCount,
            long odsCount
    ) {
        CurrentStateTableCount underTest = new CurrentStateTableCount(0.01, 0L, dataSourceCount, structuredCount, curatedCount, odsCount);
        assertFalse(underTest.countsMatch());
    }

    @ParameterizedTest
    @CsvSource({
            "0.05,1",
            "0.01,5",
    })
    void shouldUseTheLargerOfTheTolerances(double relativeTolerance, long absoluteTolerance) {
        CurrentStateTableCount underTest = new CurrentStateTableCount(relativeTolerance, absoluteTolerance, 100L, 100L, 95L, 100L);
        assertTrue(underTest.countsMatch());
    }

    @ParameterizedTest
    @MethodSource("countsThatDoNotMatch")
    void shouldGiveNoMatchForCountsThatDoNotMatch(long nomisCount, long structuredCount, long curatedCount, long odsCount) {
        CurrentStateTableCount underTest = new CurrentStateTableCount(0.0, 0L, nomisCount, structuredCount, curatedCount, odsCount);
        assertFalse(underTest.countsMatch());
    }

    @Test
    void shouldGiveMatchForCountsThatMatchWithDisabledODS() {
        CurrentStateTableCount underTest = new CurrentStateTableCount(0.0, 0L, 1L, 1L, 1L);
        assertTrue(underTest.countsMatch());
    }

    @ParameterizedTest
    @MethodSource("countsThatDoNotMatchDisabledOds")
    void shouldGiveNoMatchForCountsThatDoNotMatchWithDisabledODS(long nomisCount, long structuredCount, long curatedCount) {
        CurrentStateTableCount underTest = new CurrentStateTableCount(0.0, 0L, nomisCount, structuredCount, curatedCount);
        assertFalse(underTest.countsMatch());
    }

    @Test
    void shouldProvideSummaryWhenCountsMatch() {
        CurrentStateTableCount underTest = new CurrentStateTableCount(0.0, 0L, 1L, 1L, 1L, 1L);
        String expected = "Data Source: 1, Structured Zone: 1, Curated Zone: 1, Operational DataStore: 1	 - MATCH";
        assertEquals(expected, underTest.summary());
    }

    @Test
    void shouldProvideSummaryWhenCountsAreWithinTolerance() {
        CurrentStateTableCount underTest = new CurrentStateTableCount(0.0, 1L, 1L, 2L, 1L, 1L);
        String expected = "Data Source: 1, Structured Zone: 2, Curated Zone: 1, Operational DataStore: 1	 - MATCH (within tolerance)";
        assertEquals(expected, underTest.summary());
    }

    @Test
    void shouldProvideSummaryWhenCountsMatchWithDisabledODS() {
        CurrentStateTableCount underTest = new CurrentStateTableCount(0.0, 0L, 1L, 1L, 1L);
        String expected = "Data Source: 1, Structured Zone: 1, Curated Zone: 1, Operational DataStore: skipped	 - MATCH";
        assertEquals(expected, underTest.summary());
    }

    @Test
    void shouldProvideSummaryWhenCountsDoNotMatch() {
        CurrentStateTableCount underTest = new CurrentStateTableCount(0.0, 0L, 1L, 2L, 1L, 1L);
        String expected = "Data Source: 1, Structured Zone: 2, Curated Zone: 1, Operational DataStore: 1	 - MISMATCH";
        assertEquals(expected, underTest.summary());
    }

    @Test
    void shouldProvideSummaryWhenCountsDoNotMatchWithDisabledODS() {
        CurrentStateTableCount underTest = new CurrentStateTableCount(0.0, 0L, 1L, 2L, 3L);
        String expected = "Data Source: 1, Structured Zone: 2, Curated Zone: 3, Operational DataStore: skipped	 - MISMATCH";
        assertEquals(expected, underTest.summary());
    }

    @Test
    void shouldBeEqualToAnotherWithSameValues() {
        CurrentStateTableCount underTest1 = new CurrentStateTableCount(0.0, 0L, 1L, 1L, 1L, 1L);
        CurrentStateTableCount underTest2 = new CurrentStateTableCount(0.0, 0L, 1L, 1L, 1L, 1L);

        assertEquals(underTest1, underTest2);
    }

    @Test
    void shouldBeEqualToAnotherWithSameValuesNoODSCount() {
        CurrentStateTableCount underTest1 = new CurrentStateTableCount(0.0, 0L, 1L, 1L, 1L);
        CurrentStateTableCount underTest2 = new CurrentStateTableCount(0.0, 0L, 1L, 1L, 1L);

        assertEquals(underTest1, underTest2);
    }

    @Test
    void shouldNotBeEqualToAnotherWithDifferentValues() {
        CurrentStateTableCount underTest1 = new CurrentStateTableCount(0.0, 0L, 1L, 1L, 1L, 1L);
        CurrentStateTableCount underTest2 = new CurrentStateTableCount(0.0, 0L, 1L, 1L, 1L);

        assertNotEquals(underTest1, underTest2);
    }
}