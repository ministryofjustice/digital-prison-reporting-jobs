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

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    private SparkSession sparkSession;
    @Mock
    private SourceReference sourceReference1;
    @Mock
    private SourceReference sourceReference2;
    @Mock
    private CurrentStateTotalCounts currentStateResult;
    @Mock
    private ChangeDataTotalCounts changeDataResult;

    @InjectMocks
    private DataReconciliationService underTest;

    @Test
    void shouldGetCurrentStateCounts() {
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

        CurrentStateTotalCounts results = underTest.reconcileData(sparkSession).getCurrentStateTotalCounts();

        assertEquals(currentStateResult, results);
        verify(currentStateCountService, times(1)).currentStateCounts(sparkSession, allSourceReferences);
    }

    @Test
    void shouldGetChangeDataCounts() {
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

        ChangeDataTotalCounts results = underTest.reconcileData(sparkSession).getChangeDataTotalCounts();

        assertEquals(changeDataResult, results);
        verify(changeDataCountService, times(1)).changeDataCounts(sparkSession, allSourceReferences, DMS_TASK_ID);
    }

    @Test
    void shouldGetConfiguredTables() {
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
}