package uk.gov.justice.digital.service.datareconciliation;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
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
import static org.mockito.Mockito.eq;

@ExtendWith(MockitoExtension.class)
class ChangeDataCountServiceTest {

    private static final String DMS_TASK_ID = "dms-task-id";
    private static final String DMS_CDC_TASK_ID = "dms-cdc-task-id";
    @Mock
    private JobArguments jobArguments;
    @Mock
    private DmsChangeDataCountService dmsChangeDataCountService;
    @Mock
    private RawChangeDataCountService rawChangeDataCountService;
    @Mock
    private SourceReference sourceReference;
    @Mock
    private SparkSession sparkSession;

    private DmsChangeDataCountsPair dmsChangeDataCountsPair;
    private DmsChangeDataCountsPair dmsCdcChangeDataCountsPair;
    private List<SourceReference> sourceReferences;
    private Map<String, ChangeDataTableCount> rawChangeDataCounts;

    Map<String, ChangeDataTableCount> dmsChangeDataCounts = new HashMap<>();
    Map<String, ChangeDataTableCount> dmsAppliedChangeDataCounts = new HashMap<>();
    Map<String, ChangeDataTableCount> dmsCdcChangeDataCounts = new HashMap<>();
    Map<String, ChangeDataTableCount> dmsCdcAppliedChangeDataCounts = new HashMap<>();

    ChangeDataTableCount loadChangeDataCounts = new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L);
    ChangeDataTableCount loadAppliedChangeDataCounts = new ChangeDataTableCount(0.0, 0L, 1L, 2L, 3L);
    ChangeDataTableCount cdcChangeDataCounts = new ChangeDataTableCount(0.0, 0L, 4L, 5L, 6L);
    ChangeDataTableCount cdcAppliedChangeDataCounts = new ChangeDataTableCount(0.0, 0L, 7L, 7L, 7L);

    @InjectMocks
    private ChangeDataCountService underTest;

    @BeforeEach
    void setUp() {
        dmsChangeDataCounts.put("table", loadChangeDataCounts);
        dmsAppliedChangeDataCounts.put("table", loadAppliedChangeDataCounts);
        dmsCdcChangeDataCounts.put("table", cdcChangeDataCounts);
        dmsCdcAppliedChangeDataCounts.put("table", cdcAppliedChangeDataCounts);

        dmsChangeDataCountsPair = new DmsChangeDataCountsPair(dmsChangeDataCounts, dmsAppliedChangeDataCounts);
        dmsCdcChangeDataCountsPair = new DmsChangeDataCountsPair(dmsCdcChangeDataCounts, dmsCdcAppliedChangeDataCounts);

        rawChangeDataCounts = new HashMap<>();
        rawChangeDataCounts.put("table", new ChangeDataTableCount(0.0, 0L, 1L, 1L, 1L));

        sourceReferences = Collections.singletonList(sourceReference);
    }

    @Test
    void shouldPopulateResultsFromRetrievedData() {
        when(jobArguments.isSplitPipeline()).thenReturn(false);
        when(jobArguments.getDmsTaskId()).thenReturn(DMS_TASK_ID);
        when(dmsChangeDataCountService.dmsChangeDataCounts(any(), any())).thenReturn(dmsChangeDataCountsPair);
        when(rawChangeDataCountService.changeDataCounts(any(), any())).thenReturn(rawChangeDataCounts);

        ChangeDataTotalCounts result = (ChangeDataTotalCounts) underTest.changeDataCounts(sparkSession, sourceReferences);

        assertEquals(dmsChangeDataCountsPair.getDmsChangeDataCounts(), result.getDmsCounts());
        assertEquals(dmsChangeDataCountsPair.getDmsAppliedChangeDataCounts(), result.getDmsAppliedCounts());
        assertEquals(rawChangeDataCounts, result.getRawZoneCounts());
    }

    @Test
    void shouldPopulateResultsFromRetrievedDataForSplitPipeline() {
        when(jobArguments.isSplitPipeline()).thenReturn(true);
        when(jobArguments.getDmsTaskId()).thenReturn(DMS_TASK_ID);
        when(jobArguments.getCdcDmsTaskId()).thenReturn(DMS_CDC_TASK_ID);
        when(dmsChangeDataCountService.dmsChangeDataCounts(any(), eq(DMS_TASK_ID))).thenReturn(dmsChangeDataCountsPair);
        when(dmsChangeDataCountService.dmsChangeDataCounts(any(), eq(DMS_CDC_TASK_ID))).thenReturn(dmsCdcChangeDataCountsPair);
        when(rawChangeDataCountService.changeDataCounts(any(), any())).thenReturn(rawChangeDataCounts);

        ChangeDataTotalCounts result = (ChangeDataTotalCounts) underTest.changeDataCounts(sparkSession, sourceReferences);

        assertEquals(
                Collections.singletonMap("table", new ChangeDataTableCount(0.0, 0L, 5L, 6L, 7L)),
                result.getDmsCounts()
        );
        assertEquals(
                Collections.singletonMap("table", loadAppliedChangeDataCounts.combineCounts(cdcAppliedChangeDataCounts)),
                result.getDmsAppliedCounts()
        );
        assertEquals(rawChangeDataCounts, result.getRawZoneCounts());
    }

    @Test
    void shouldRetrieveDmsChangeDataCounts() {
        when(jobArguments.isSplitPipeline()).thenReturn(false);
        when(jobArguments.getDmsTaskId()).thenReturn(DMS_TASK_ID);
        when(dmsChangeDataCountService.dmsChangeDataCounts(any(), any())).thenReturn(dmsChangeDataCountsPair);

        underTest.changeDataCounts(sparkSession, sourceReferences);

        verify(dmsChangeDataCountService, times(1)).dmsChangeDataCounts(sourceReferences, DMS_TASK_ID);
    }

    @Test
    void shouldRetrieveRawChangeDataCounts() {
        when(jobArguments.isSplitPipeline()).thenReturn(false);
        when(jobArguments.getDmsTaskId()).thenReturn(DMS_TASK_ID);
        when(dmsChangeDataCountService.dmsChangeDataCounts(any(), any())).thenReturn(dmsChangeDataCountsPair);

        underTest.changeDataCounts(sparkSession, sourceReferences);

        verify(rawChangeDataCountService, times(1)).changeDataCounts(sparkSession, sourceReferences);
    }
}