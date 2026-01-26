package uk.gov.justice.digital.service.datareconciliation;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTotalCounts;
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateTotalCounts;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResult;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;
import uk.gov.justice.digital.service.datareconciliation.model.PrimaryKeyReconciliationCounts;
import uk.gov.justice.digital.service.metrics.MetricReportingService;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.service.datareconciliation.model.ReconciliationCheck.CHANGE_DATA_COUNTS;
import static uk.gov.justice.digital.service.datareconciliation.model.ReconciliationCheck.CURRENT_STATE_COUNTS;
import static uk.gov.justice.digital.service.datareconciliation.model.ReconciliationCheck.PRIMARY_KEY_RECONCILIATION;

@ExtendWith(MockitoExtension.class)
class DataReconciliationServiceTest {

    private static final String DMS_TASK_ID = "dms-task-id";

    @Mock
    private JobArguments jobArguments;
    @Mock
    private ConfigService configService;
    @Mock
    private SourceReferenceService sourceReferenceService;
    @Mock
    private CurrentStateCountService currentStateCountService;
    @Mock
    private ChangeDataCountService changeDataCountService;
    @Mock
    private PrimaryKeyReconciliationService primaryKeyReconciliationService;
    @Mock
    private SparkSession sparkSession;
    @Mock
    private SourceReference sourceReference1;
    @Mock
    private SourceReference sourceReference2;
    @Mock
    private CurrentStateTotalCounts currentStateResult;
    @Mock
    private ChangeDataTotalCounts changeDataResult;
    @Mock
    private PrimaryKeyReconciliationCounts primaryKeyResult;
    @Mock
    private MetricReportingService metricReportingService;

    @InjectMocks
    private DataReconciliationService underTest;

    @Test
    void shouldGetCurrentStateCounts() {
        when(jobArguments.getReconciliationChecksToRun()).thenReturn(ImmutableSet.of(CURRENT_STATE_COUNTS));
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema", "table1"),
                ImmutablePair.of("schema", "table2")
        );
        when(configService.getConfiguredTables(any())).thenReturn(configuredTables);
        List<SourceReference> allSourceReferences = Arrays.asList(
                sourceReference1, sourceReference2
        );
        when(sourceReferenceService.getAllSourceReferences(configuredTables)).thenReturn(allSourceReferences);
        when(currentStateCountService.currentStateCounts(sparkSession, allSourceReferences)).thenReturn(currentStateResult);

        List<DataReconciliationResult> results = ((DataReconciliationResults) underTest.reconcileData(sparkSession)).getResults();

        assertEquals(1, results.size());
        assertTrue(results.contains(currentStateResult));
        verify(currentStateCountService, times(1)).currentStateCounts(sparkSession, allSourceReferences);
    }

    @Test
    void shouldGetChangeDataCounts() {
        when(jobArguments.getReconciliationChecksToRun()).thenReturn(ImmutableSet.of(CHANGE_DATA_COUNTS));
        when(jobArguments.getDmsTaskId()).thenReturn(DMS_TASK_ID);
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema", "table1"),
                ImmutablePair.of("schema", "table2")
        );
        when(configService.getConfiguredTables(any())).thenReturn(configuredTables);
        List<SourceReference> allSourceReferences = Arrays.asList(
                sourceReference1, sourceReference2
        );
        when(sourceReferenceService.getAllSourceReferences(configuredTables)).thenReturn(allSourceReferences);
        when(changeDataCountService.changeDataCounts(sparkSession, allSourceReferences, DMS_TASK_ID)).thenReturn(changeDataResult);

        List<DataReconciliationResult> results = ((DataReconciliationResults) underTest.reconcileData(sparkSession)).getResults();

        assertEquals(1, results.size());
        assertTrue(results.contains(changeDataResult));
        verify(changeDataCountService, times(1)).changeDataCounts(sparkSession, allSourceReferences, DMS_TASK_ID);
    }

    @Test
    void shouldGetPrimaryKeyReconciliationCounts() {
        when(jobArguments.getReconciliationChecksToRun()).thenReturn(ImmutableSet.of(PRIMARY_KEY_RECONCILIATION));
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema", "table1"),
                ImmutablePair.of("schema", "table2")
        );
        when(configService.getConfiguredTables(any())).thenReturn(configuredTables);
        List<SourceReference> allSourceReferences = Arrays.asList(
                sourceReference1, sourceReference2
        );
        when(sourceReferenceService.getAllSourceReferences(configuredTables)).thenReturn(allSourceReferences);
        when(primaryKeyReconciliationService.primaryKeyReconciliation(sparkSession, allSourceReferences)).thenReturn(primaryKeyResult);

        List<DataReconciliationResult> results = ((DataReconciliationResults) underTest.reconcileData(sparkSession)).getResults();

        assertEquals(1, results.size());
        assertTrue(results.contains(primaryKeyResult));
        verify(primaryKeyReconciliationService, times(1)).primaryKeyReconciliation(sparkSession, allSourceReferences);
    }

    @Test
    void shouldGetCurrentStateCountsAndChangeDataCounts() {
        when(jobArguments.getReconciliationChecksToRun()).thenReturn(ImmutableSet.of(CURRENT_STATE_COUNTS, CHANGE_DATA_COUNTS));
        when(jobArguments.getDmsTaskId()).thenReturn(DMS_TASK_ID);
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema", "table1"),
                ImmutablePair.of("schema", "table2")
        );
        when(configService.getConfiguredTables(any())).thenReturn(configuredTables);
        List<SourceReference> allSourceReferences = Arrays.asList(
                sourceReference1, sourceReference2
        );
        when(sourceReferenceService.getAllSourceReferences(configuredTables)).thenReturn(allSourceReferences);
        when(currentStateCountService.currentStateCounts(sparkSession, allSourceReferences)).thenReturn(currentStateResult);
        when(changeDataCountService.changeDataCounts(sparkSession, allSourceReferences, DMS_TASK_ID)).thenReturn(changeDataResult);

        List<DataReconciliationResult> results = ((DataReconciliationResults) underTest.reconcileData(sparkSession)).getResults();

        assertEquals(2, results.size());
        assertTrue(results.contains(currentStateResult));
        assertTrue(results.contains(changeDataResult));
        verify(currentStateCountService, times(1)).currentStateCounts(sparkSession, allSourceReferences);
        verify(changeDataCountService, times(1)).changeDataCounts(sparkSession, allSourceReferences, DMS_TASK_ID);
    }

    @Test
    void shouldRunJustConfiguredReconciliationChecks() {
        when(jobArguments.getDmsTaskId()).thenReturn(DMS_TASK_ID);
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema", "table1"),
                ImmutablePair.of("schema", "table2")
        );
        when(configService.getConfiguredTables(any())).thenReturn(configuredTables);
        List<SourceReference> allSourceReferences = Arrays.asList(
                sourceReference1, sourceReference2
        );
        when(sourceReferenceService.getAllSourceReferences(configuredTables)).thenReturn(allSourceReferences);

        // Verify running with different configured reconciliation checks, resetting mocks between each run

        when(jobArguments.getReconciliationChecksToRun()).thenReturn(ImmutableSet.of(CURRENT_STATE_COUNTS));
        underTest.reconcileData(sparkSession);
        verify(currentStateCountService, times(1)).currentStateCounts(sparkSession, allSourceReferences);
        verify(changeDataCountService, times(0)).changeDataCounts(sparkSession, allSourceReferences, DMS_TASK_ID);

        reset(currentStateCountService);
        reset(changeDataCountService);
        when(jobArguments.getReconciliationChecksToRun()).thenReturn(ImmutableSet.of(CHANGE_DATA_COUNTS));
        underTest.reconcileData(sparkSession);
        verify(currentStateCountService, times(0)).currentStateCounts(sparkSession, allSourceReferences);
        verify(changeDataCountService, times(1)).changeDataCounts(sparkSession, allSourceReferences, DMS_TASK_ID);

        reset(currentStateCountService);
        reset(changeDataCountService);
        when(jobArguments.getReconciliationChecksToRun()).thenReturn(ImmutableSet.of(CURRENT_STATE_COUNTS, CHANGE_DATA_COUNTS));
        underTest.reconcileData(sparkSession);
        verify(currentStateCountService, times(1)).currentStateCounts(sparkSession, allSourceReferences);
        verify(changeDataCountService, times(1)).changeDataCounts(sparkSession, allSourceReferences, DMS_TASK_ID);
    }

    @Test
    void shouldGetConfiguredTables() {
        when(jobArguments.getReconciliationChecksToRun()).thenReturn(ImmutableSet.of(CURRENT_STATE_COUNTS, CHANGE_DATA_COUNTS));
        when(jobArguments.getConfigKey()).thenReturn("config-key");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema", "table1"),
                ImmutablePair.of("schema", "table2")
        );
        when(configService.getConfiguredTables(any())).thenReturn(configuredTables);
        List<SourceReference> allSourceReferences = Arrays.asList(
                sourceReference1, sourceReference2
        );
        when(sourceReferenceService.getAllSourceReferences(configuredTables)).thenReturn(allSourceReferences);
        when(currentStateCountService.currentStateCounts(sparkSession, allSourceReferences)).thenReturn(currentStateResult);

        underTest.reconcileData(sparkSession);

        verify(configService, times(1)).getConfiguredTables("config-key");
    }

    @Test
    void shouldGetAllSourceReferences() {
        when(jobArguments.getReconciliationChecksToRun()).thenReturn(ImmutableSet.of(CURRENT_STATE_COUNTS, CHANGE_DATA_COUNTS));
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema", "table1"),
                ImmutablePair.of("schema", "table2")
        );
        when(configService.getConfiguredTables(any())).thenReturn(configuredTables);
        List<SourceReference> allSourceReferences = Arrays.asList(
                sourceReference1, sourceReference2
        );
        when(sourceReferenceService.getAllSourceReferences(configuredTables)).thenReturn(allSourceReferences);
        when(currentStateCountService.currentStateCounts(sparkSession, allSourceReferences)).thenReturn(currentStateResult);

        underTest.reconcileData(sparkSession);

        verify(sourceReferenceService, times(1)).getAllSourceReferences(configuredTables);
    }

    @Test
    void shouldReportMetrics() {
        when(jobArguments.getReconciliationChecksToRun()).thenReturn(ImmutableSet.of(CURRENT_STATE_COUNTS, CHANGE_DATA_COUNTS));
        when(jobArguments.getDmsTaskId()).thenReturn(DMS_TASK_ID);
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema", "table1"),
                ImmutablePair.of("schema", "table2")
        );
        when(configService.getConfiguredTables(any())).thenReturn(configuredTables);
        List<SourceReference> allSourceReferences = Arrays.asList(
                sourceReference1, sourceReference2
        );
        when(sourceReferenceService.getAllSourceReferences(configuredTables)).thenReturn(allSourceReferences);
        when(currentStateCountService.currentStateCounts(sparkSession, allSourceReferences)).thenReturn(currentStateResult);
        when(changeDataCountService.changeDataCounts(sparkSession, allSourceReferences, DMS_TASK_ID)).thenReturn(changeDataResult);

        DataReconciliationResults results = ((DataReconciliationResults) underTest.reconcileData(sparkSession));

        verify(metricReportingService, times(1)).reportDataReconciliationResults(results);
    }
}
