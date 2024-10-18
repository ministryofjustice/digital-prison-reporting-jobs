package uk.gov.justice.digital.service.datareconciliation;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTableCount;
import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTotalCounts;
import uk.gov.justice.digital.service.datareconciliation.model.DmsChangeDataCountsPair;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ChangeDataCountServiceTest {

    private static final String DMS_TASK_ID = "dms-task-id";
    @Mock
    private DmsChangeDataCountService dmsChangeDataCountService;
    @Mock
    private RawChangeDataCountService rawChangeDataCountService;
    @Mock
    private SourceReference sourceReference;
    @Mock
    private SparkSession sparkSession;

    private DmsChangeDataCountsPair dmsChangeDataCountsPair;
    private List<SourceReference> sourceReferences;
    private Map<String, ChangeDataTableCount> rawChangeDataCounts;

    @InjectMocks
    private ChangeDataCountService underTest;

    @BeforeEach
    void setUp() {
        Map<String, ChangeDataTableCount> dmsChangeDataCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedChangeDataCounts = new HashMap<>();
        dmsChangeDataCounts.put("table", new ChangeDataTableCount(1L, 1L, 1L));
        dmsAppliedChangeDataCounts.put("table", new ChangeDataTableCount(1L, 2L, 3L));
        dmsChangeDataCountsPair = new DmsChangeDataCountsPair(dmsChangeDataCounts, dmsAppliedChangeDataCounts);

        rawChangeDataCounts = new HashMap<>();
        rawChangeDataCounts.put("table", new ChangeDataTableCount(1L, 1L, 1L));

        sourceReferences = Collections.singletonList(sourceReference);
    }

    @Test
    void shouldPopulateResultsFromRetrievedData() {
        when(dmsChangeDataCountService.dmsChangeDataCounts(any(), any())).thenReturn(dmsChangeDataCountsPair);
        when(rawChangeDataCountService.changeDataCounts(any(), any())).thenReturn(rawChangeDataCounts);

        ChangeDataTotalCounts result = underTest.changeDataCounts(sparkSession, sourceReferences, DMS_TASK_ID);

        assertEquals(dmsChangeDataCountsPair.getDmsChangeDataCounts(), result.getDmsCounts());
        assertEquals(dmsChangeDataCountsPair.getDmsAppliedChangeDataCounts(), result.getDmsAppliedCounts());
        assertEquals(rawChangeDataCounts, result.getRawZoneCounts());
    }

    @Test
    void shouldRetrieveDmsChangeDataCounts() {
        when(dmsChangeDataCountService.dmsChangeDataCounts(any(), any())).thenReturn(dmsChangeDataCountsPair);

        underTest.changeDataCounts(sparkSession, sourceReferences, DMS_TASK_ID);

        verify(dmsChangeDataCountService, times(1)).dmsChangeDataCounts(sourceReferences, DMS_TASK_ID);
    }

    @Test
    void shouldRetrieveRawChangeDataCounts() {
        when(dmsChangeDataCountService.dmsChangeDataCounts(any(), any())).thenReturn(dmsChangeDataCountsPair);

        underTest.changeDataCounts(sparkSession, sourceReferences, DMS_TASK_ID);

        verify(rawChangeDataCountService, times(1)).changeDataCounts(sparkSession, sourceReferences);
    }
}