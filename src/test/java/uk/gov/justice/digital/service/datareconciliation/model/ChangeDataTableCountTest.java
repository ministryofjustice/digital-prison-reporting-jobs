package uk.gov.justice.digital.service.datareconciliation.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

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

}