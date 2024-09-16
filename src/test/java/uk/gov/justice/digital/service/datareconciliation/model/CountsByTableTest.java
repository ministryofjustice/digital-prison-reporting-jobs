package uk.gov.justice.digital.service.datareconciliation.model;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class CountsByTableTest {

    private static final String tableName = "table1";

    @Test
    void shouldPutAndGet() {

        CountsByTable<Long> underTest = new CountsByTable<>();

        assertNull(underTest.get(tableName));

        underTest.put(tableName, 1L);
        assertEquals(1L, underTest.get(tableName));
    }

    @Test
    void shouldGetEntrySet() {
        CountsByTable<Long> underTest = new CountsByTable<>();
        underTest.put(tableName, 1L);

        Set<Map.Entry<String, Long>> entries = underTest.entrySet();

        assertEquals(1, entries.size());

        Map.Entry<String, Long> entry = entries.iterator().next();
        assertEquals(tableName, entry.getKey());
        assertEquals(1L, entry.getValue());
    }

    @Test
    void shouldGetKeySet() {
        CountsByTable<Long> underTest = new CountsByTable<>();
        underTest.put(tableName, 1L);

        Set<String> keySet = underTest.keySet();
        assertEquals(1, keySet.size());
        assertEquals(tableName, keySet.iterator().next());
    }

    @Test
    void shouldBeEqualToAnotherWithSameValues() {
        CountsByTable<Long> underTest1 = new CountsByTable<>();
        CountsByTable<Long> underTest2 = new CountsByTable<>();
        underTest1.put(tableName, 1L);
        underTest2.put(tableName, 1L);

        assertEquals(underTest1, underTest2);
    }

    @Test
    void shouldNotBeEqualToAnotherWithDifferentValues() {
        CountsByTable<Long> underTest1 = new CountsByTable<>();
        CountsByTable<Long> underTest2 = new CountsByTable<>();

        underTest1.put(tableName, 1L);
        underTest2.put(tableName, 2L);
        assertNotEquals(underTest1, underTest2);
    }

    @Test
    void shouldNotBeEqualToAnotherWithDifferentTables() {
        CountsByTable<Long> underTest1 = new CountsByTable<>();
        CountsByTable<Long> underTest2 = new CountsByTable<>();

        underTest1.put(tableName, 1L);
        underTest2.put("a different table name", 1L);
        assertNotEquals(underTest1, underTest2);
    }
}