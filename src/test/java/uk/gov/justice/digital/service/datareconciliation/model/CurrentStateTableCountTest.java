package uk.gov.justice.digital.service.datareconciliation.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
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
        CurrentStateTableCount underTest = new CurrentStateTableCount(1L, 1L, 1L, 1L);
        assertTrue(underTest.countsMatch());
    }

    @ParameterizedTest
    @MethodSource("countsThatDoNotMatch")
    void shouldGiveNoMatchForCountsThatDoNotMatch(long nomisCount, long structuredCount, long curatedCount, long odsCount) {
        CurrentStateTableCount underTest = new CurrentStateTableCount(nomisCount, structuredCount, curatedCount, odsCount);
        assertFalse(underTest.countsMatch());
    }

    @Test
    void shouldGiveMatchForCountsThatMatchWithDisabledODS() {
        CurrentStateTableCount underTest = new CurrentStateTableCount(1L, 1L, 1L);
        assertTrue(underTest.countsMatch());
    }

    @ParameterizedTest
    @MethodSource("countsThatDoNotMatchDisabledOds")
    void shouldGiveNoMatchForCountsThatDoNotMatchWithDisabledODS(long nomisCount, long structuredCount, long curatedCount) {
        CurrentStateTableCount underTest = new CurrentStateTableCount(nomisCount, structuredCount, curatedCount);
        assertFalse(underTest.countsMatch());
    }

    @Test
    void shouldProvideSummaryWhenCountsMatch() {
        CurrentStateTableCount underTest = new CurrentStateTableCount(1L, 1L, 1L, 1L);
        String expected = "   MATCH: Nomis: 1, Structured Zone: 1, Curated Zone: 1, Operational DataStore: 1";
        assertEquals(expected, underTest.summary());
    }

    @Test
    void shouldProvideSummaryWhenCountsMatchWithDisabledODS() {
        CurrentStateTableCount underTest = new CurrentStateTableCount(1L, 1L, 1L);
        String expected = "   MATCH: Nomis: 1, Structured Zone: 1, Curated Zone: 1, Operational DataStore: skipped";
        assertEquals(expected, underTest.summary());
    }

    @Test
    void shouldProvideSummaryWhenCountsDoNotMatch() {
        CurrentStateTableCount underTest = new CurrentStateTableCount(1L, 2L, 1L, 1L);
        String expected = "MISMATCH: Nomis: 1, Structured Zone: 2, Curated Zone: 1, Operational DataStore: 1";
        assertEquals(expected, underTest.summary());
    }

    @Test
    void shouldProvideSummaryWhenCountsDoNotMatchWithDisabledODS() {
        CurrentStateTableCount underTest = new CurrentStateTableCount(1L, 2L, 3L);
        String expected = "MISMATCH: Nomis: 1, Structured Zone: 2, Curated Zone: 3, Operational DataStore: skipped";
        assertEquals(expected, underTest.summary());
    }

}