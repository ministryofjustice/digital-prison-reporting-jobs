package uk.gov.justice.digital.service.datareconciliation.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DmsChangeDataCountsPairTest {

    Map<String, ChangeDataTableCount> changeDataCounts1 = new HashMap<>();
    Map<String, ChangeDataTableCount> changeDataCounts2 = new HashMap<>();

    @Test
    void shouldSumDmsChangeDataCounts() {
        changeDataCounts1.put("table1", new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        changeDataCounts1.put("table2", new ChangeDataTableCount(0.0, 0L, 1L, 2L, 3L));
        changeDataCounts2.put("table2", new ChangeDataTableCount(0.0, 0L, 6L, 2L, 4L));

        DmsChangeDataCountsPair countsPair1 = new DmsChangeDataCountsPair(changeDataCounts1, new HashMap<>());
        DmsChangeDataCountsPair countsPair2 = new DmsChangeDataCountsPair(changeDataCounts2, new HashMap<>());

        countsPair1.sum(countsPair2);

        Map<String, ChangeDataTableCount> expectedCounts = new HashMap<>();
        expectedCounts.put("table1", new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        expectedCounts.put("table2", new ChangeDataTableCount(0.0, 0L, 7L, 4L, 7L));

        assertEquals(expectedCounts, countsPair1.getDmsChangeDataCounts());
    }

    @Test
    void shouldSumAppliedDmsChangeDataCounts() {
        changeDataCounts1.put("table1", new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        changeDataCounts1.put("table2", new ChangeDataTableCount(0.0, 0L, 1L, 2L, 3L));
        changeDataCounts2.put("table2", new ChangeDataTableCount(0.0, 0L, 6L, 2L, 4L));

        DmsChangeDataCountsPair countsPair1 = new DmsChangeDataCountsPair(new HashMap<>(), changeDataCounts1);
        DmsChangeDataCountsPair countsPair2 = new DmsChangeDataCountsPair(new HashMap<>(), changeDataCounts2);

        countsPair1.sum(countsPair2);

        Map<String, ChangeDataTableCount> expectedCounts = new HashMap<>();
        expectedCounts.put("table1", new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));
        expectedCounts.put("table2", new ChangeDataTableCount(0.0, 0L, 7L, 4L, 7L));

        assertEquals(expectedCounts, countsPair1.getDmsAppliedChangeDataCounts());
    }
}