package uk.gov.justice.digital.service.datareconciliation.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class ChangeDataTableRawZoneCountTest {

    static Stream<Object> countsThatDoNotMatch() {
        return Stream.of(
                new Object[]{1L, 1L, 2L},
                new Object[]{1L, 2L, 1L},
                new Object[]{2L, 1L, 1L},
                new Object[]{1L, 2L, 3L}
        );
    }

    @Test
    void shouldReturnTrueIfCountsMatch() {
        ChangeDataTableRawZoneCount underTest = new ChangeDataTableRawZoneCount(1L, 2L, 3L);
        ChangeDataTableDmsCount dmsCount = new ChangeDataTableDmsCount(1L, 2L, 3L, 1L, 2L, 3L);

        assertTrue(underTest.dmsCountsMatch(dmsCount));
    }

    @ParameterizedTest
    @MethodSource("countsThatDoNotMatch")
    void shouldReturnFalseIfInsertCountsDoNotMatch(long rawInsertCount, long dmsInsertCount, long dmsAppliedInsertCount) {
        long matchingUpdateCount = 1L;
        long matchingDeleteCount = 1L;
        ChangeDataTableRawZoneCount underTest = new ChangeDataTableRawZoneCount(rawInsertCount, matchingUpdateCount, matchingDeleteCount);
        ChangeDataTableDmsCount dmsCount =
                new ChangeDataTableDmsCount(dmsInsertCount, matchingUpdateCount, matchingDeleteCount, dmsAppliedInsertCount, matchingUpdateCount, matchingDeleteCount);

        assertFalse(underTest.dmsCountsMatch(dmsCount));
    }

    @ParameterizedTest
    @MethodSource("countsThatDoNotMatch")
    void shouldReturnFalseIfUpdateCountsDoNotMatch(long rawUpdateCount, long dmsUpdateCount, long dmsAppliedUpdateCount) {
        long matchingInsertCount = 1L;
        long matchingDeleteCount = 1L;
        ChangeDataTableRawZoneCount underTest = new ChangeDataTableRawZoneCount(matchingInsertCount, rawUpdateCount, matchingDeleteCount);
        ChangeDataTableDmsCount dmsCount =
                new ChangeDataTableDmsCount(matchingInsertCount, dmsUpdateCount, matchingDeleteCount, matchingInsertCount, dmsAppliedUpdateCount, matchingDeleteCount);

        assertFalse(underTest.dmsCountsMatch(dmsCount));
    }

    @ParameterizedTest
    @MethodSource("countsThatDoNotMatch")
    void shouldReturnFalseIfDeleteCountsDoNotMatch(long rawDeleteCount, long dmsDeleteCount, long dmsAppliedDeleteCount) {
        long matchingInsertCount = 1L;
        long matchingUpdateCount = 1L;
        ChangeDataTableRawZoneCount underTest = new ChangeDataTableRawZoneCount(matchingInsertCount, matchingUpdateCount, rawDeleteCount);
        ChangeDataTableDmsCount dmsCount =
                new ChangeDataTableDmsCount(matchingInsertCount, matchingUpdateCount, dmsDeleteCount, matchingInsertCount, matchingUpdateCount, dmsAppliedDeleteCount);

        assertFalse(underTest.dmsCountsMatch(dmsCount));
    }

    @Test
    void shouldDefaultToZeroCounts() {
        ChangeDataTableRawZoneCount underTest = new ChangeDataTableRawZoneCount();
        assertEquals(0L, underTest.getInsertCount());
        assertEquals(0L, underTest.getUpdateCount());
        assertEquals(0L, underTest.getDeleteCount());
    }

    @Test
    void shouldProduceSummary() {
        ChangeDataTableRawZoneCount underTest = new ChangeDataTableRawZoneCount();
        underTest.setInsertCount(1L);
        underTest.setUpdateCount(2L);
        underTest.setDeleteCount(3L);
        String actual = underTest.summary();
        String expected = "Inserts: 1, Updates: 2, Deletes: 3";
        assertEquals(expected, actual);
    }

    @Test
    void shouldCombineCounts() {
        ChangeDataTableRawZoneCount underTest1 = new ChangeDataTableRawZoneCount(1L, 2L, 3L);
        ChangeDataTableRawZoneCount underTest2 = new ChangeDataTableRawZoneCount(2L, 3L, 4L);

        ChangeDataTableRawZoneCount combined = underTest1.combineCounts(underTest2);

        assertEquals(3L, combined.getInsertCount());
        assertEquals(5L, combined.getUpdateCount());
        assertEquals(7L, combined.getDeleteCount());
    }

}