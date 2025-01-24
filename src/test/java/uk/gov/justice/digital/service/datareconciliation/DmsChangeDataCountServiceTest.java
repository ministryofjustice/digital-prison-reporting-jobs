package uk.gov.justice.digital.service.datareconciliation;

import com.amazonaws.services.databasemigrationservice.model.TableStatistics;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.dms.DmsClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.DmsClientException;
import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTableCount;
import uk.gov.justice.digital.service.datareconciliation.model.DmsChangeDataCountsPair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DmsChangeDataCountServiceTest {
    private static final String DMS_TASK_ID = "dms-task-id";
    private static final SourceReference sourceReference1 = new SourceReference(
            "key",
            "namespace",
            "source",
            "table1",
            null,
            "",
            null,
            null
    );
    private static final SourceReference sourceReference2 = new SourceReference(
            "key",
            "namespace",
            "source",
            "table2",
            null,
            "",
            null,
            null
    );
    private static final SourceReference sourceReference3 = new SourceReference(
            "key",
            "namespace",
            "source",
            "table3",
            null,
            "",
            null,
            null
    );

    private static final TableStatistics tableStats1 = new TableStatistics()
            .withTableName("table1")
            .withSchemaName("schema")
            .withFullLoadRows(100L)
            .withInserts(1L)
            .withAppliedInserts(2L)
            .withUpdates(3L)
            .withAppliedUpdates(4L)
            .withDeletes(5L)
            .withAppliedDeletes(6L);
    private static final TableStatistics tableStats2 = new TableStatistics()
            .withTableName("table2")
            .withSchemaName("schema")
            .withFullLoadRows(100L)
            .withInserts(7L)
            .withAppliedInserts(8L)
            .withUpdates(9L)
            .withAppliedUpdates(10L)
            .withDeletes(11L)
            .withAppliedDeletes(12L);
    private static final TableStatistics tableStats3 = new TableStatistics()
            .withTableName("table3")
            .withSchemaName("schema")
            .withFullLoadRows(100L)
            .withInserts(13L)
            .withAppliedInserts(14L)
            .withUpdates(15L)
            .withAppliedUpdates(16L)
            .withDeletes(17L)
            .withAppliedDeletes(18L);

    private static final Map<String, ChangeDataTableCount> expectedDmsChangeDataCounts = new HashMap<>();
    private static final Map<String, ChangeDataTableCount> expectedDmsAppliedChangeDataCounts = new HashMap<>();

    @Mock
    private DmsClient dmsClient;
    @Mock
    private JobArguments jobArguments;

    @InjectMocks
    private DmsChangeDataCountService underTest;

    @BeforeAll
    static void beforeAll() {
        expectedDmsChangeDataCounts.put("source.table1", new ChangeDataTableCount(
                0.0, 0L, 101L, 3L, 5L
        ));
        expectedDmsChangeDataCounts.put("source.table2", new ChangeDataTableCount(
                0.0, 0L, 107L, 9L, 11L
        ));
        expectedDmsAppliedChangeDataCounts.put("source.table1", new ChangeDataTableCount(
                0.0, 0L, 102L, 4L, 6L
        ));
        expectedDmsAppliedChangeDataCounts.put("source.table2", new ChangeDataTableCount(
                0.0, 0L, 108L, 10L, 12L
        ));
    }

    @Test
    void shouldReturnTheCountsReportedByDmsApi() {

        List<SourceReference> sourceReferences = Arrays.asList(sourceReference1, sourceReference2);
        List<TableStatistics> dmsTableStatistics = Arrays.asList(tableStats1, tableStats2);

        when(dmsClient.getReplicationTaskTableStatistics(DMS_TASK_ID)).thenReturn(dmsTableStatistics);

        DmsChangeDataCountsPair results = underTest.dmsChangeDataCounts(sourceReferences, DMS_TASK_ID);

        assertEquals(expectedDmsChangeDataCounts, results.getDmsChangeDataCounts());
        assertEquals(expectedDmsAppliedChangeDataCounts, results.getDmsAppliedChangeDataCounts());

        verify(dmsClient, times(1)).getReplicationTaskTableStatistics(DMS_TASK_ID);
    }

    @Test
    void shouldPopulateTolerancesInChangeDataCounts() {
        double relativeTolerance = 0.15;
        long absoluteTolerance = 7L;

        List<SourceReference> sourceReferences = Arrays.asList(sourceReference1, sourceReference2);
        List<TableStatistics> dmsTableStatistics = Arrays.asList(tableStats1, tableStats2);

        when(dmsClient.getReplicationTaskTableStatistics(DMS_TASK_ID)).thenReturn(dmsTableStatistics);
        when(jobArguments.getReconciliationChangeDataCountsToleranceRelativePercentage()).thenReturn(relativeTolerance);
        when(jobArguments.getReconciliationChangeDataCountsToleranceAbsolute()).thenReturn(absoluteTolerance);

        DmsChangeDataCountsPair results = underTest.dmsChangeDataCounts(sourceReferences, DMS_TASK_ID);
        ChangeDataTableCount table1Count = results.getDmsChangeDataCounts().get("source.table1");
        ChangeDataTableCount table2Count = results.getDmsChangeDataCounts().get("source.table2");
        assertEquals(relativeTolerance, table1Count.getRelativeTolerance());
        assertEquals(absoluteTolerance, table1Count.getAbsoluteTolerance());
        assertEquals(relativeTolerance, table2Count.getRelativeTolerance());
        assertEquals(absoluteTolerance, table2Count.getAbsoluteTolerance());
    }

    @Test
    void shouldPopulateTolerancesInAppliedChangeDataCounts() {
        double relativeTolerance = 0.15;
        long absoluteTolerance = 7L;

        List<SourceReference> sourceReferences = Arrays.asList(sourceReference1, sourceReference2);
        List<TableStatistics> dmsTableStatistics = Arrays.asList(tableStats1, tableStats2);

        when(dmsClient.getReplicationTaskTableStatistics(DMS_TASK_ID)).thenReturn(dmsTableStatistics);
        when(jobArguments.getReconciliationChangeDataCountsToleranceRelativePercentage()).thenReturn(relativeTolerance);
        when(jobArguments.getReconciliationChangeDataCountsToleranceAbsolute()).thenReturn(absoluteTolerance);

        DmsChangeDataCountsPair results = underTest.dmsChangeDataCounts(sourceReferences, DMS_TASK_ID);
        ChangeDataTableCount table1Count = results.getDmsAppliedChangeDataCounts().get("source.table1");
        ChangeDataTableCount table2Count = results.getDmsAppliedChangeDataCounts().get("source.table2");
        assertEquals(relativeTolerance, table1Count.getRelativeTolerance());
        assertEquals(absoluteTolerance, table1Count.getAbsoluteTolerance());
        assertEquals(relativeTolerance, table2Count.getRelativeTolerance());
        assertEquals(absoluteTolerance, table2Count.getAbsoluteTolerance());
    }

    @Test
    void shouldIgnoreTableStatisticsWithoutCorrespondingSourceReference() {

        List<SourceReference> sourceReferences = Arrays.asList(sourceReference1, sourceReference2);
        List<TableStatistics> dmsTableStatistics = Arrays.asList(tableStats1, tableStats2, tableStats3);

        when(dmsClient.getReplicationTaskTableStatistics(DMS_TASK_ID)).thenReturn(dmsTableStatistics);

        DmsChangeDataCountsPair results = underTest.dmsChangeDataCounts(sourceReferences, DMS_TASK_ID);

        assertEquals(expectedDmsChangeDataCounts, results.getDmsChangeDataCounts());
        assertEquals(expectedDmsAppliedChangeDataCounts, results.getDmsAppliedChangeDataCounts());
    }

    @Test
    void shouldIgnoreSourceReferencesWithoutCorrespondingTableStatistics() {

        List<SourceReference> sourceReferences = Arrays.asList(sourceReference1, sourceReference2, sourceReference3);
        List<TableStatistics> dmsTableStatistics = Arrays.asList(tableStats1, tableStats2);

        when(dmsClient.getReplicationTaskTableStatistics(DMS_TASK_ID)).thenReturn(dmsTableStatistics);

        DmsChangeDataCountsPair results = underTest.dmsChangeDataCounts(sourceReferences, DMS_TASK_ID);

        assertEquals(expectedDmsChangeDataCounts, results.getDmsChangeDataCounts());
        assertEquals(expectedDmsAppliedChangeDataCounts, results.getDmsAppliedChangeDataCounts());
    }

    @Test
    void shouldFailIfDmsClientFails() {
        List<SourceReference> sourceReferences = Arrays.asList(sourceReference1, sourceReference2);
        when(dmsClient.getReplicationTaskTableStatistics(DMS_TASK_ID)).thenThrow(new DmsClientException(""));
        assertThrows(DmsClientException.class, () -> underTest.dmsChangeDataCounts(sourceReferences, DMS_TASK_ID));
    }
}