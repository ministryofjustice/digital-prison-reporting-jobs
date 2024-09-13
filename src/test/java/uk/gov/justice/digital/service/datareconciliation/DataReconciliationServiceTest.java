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
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateTableCount;
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateTotalCounts;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataReconciliationServiceTest {

    @Mock
    private JobArguments jobArguments;
    @Mock
    private ConfigService configService;
    @Mock
    private SourceReferenceService sourceReferenceService;
    @Mock
    private CurrentStateCountService currentStateCountService;
    @Mock
    private SparkSession sparkSession;
    @Mock
    private SourceReference sourceReference1;
    @Mock
    private SourceReference sourceReference2;
    @Mock
    private CurrentStateTableCount result1;
    @Mock
    private CurrentStateTableCount result2;

    @InjectMocks
    private DataReconciliationService underTest;

    @Test
    void shouldGetCurrentStateCounts() {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema", "table1"),
                ImmutablePair.of("schema", "table2")
        );
        when(configService.getConfiguredTables(any())).thenReturn(configuredTables);
        when(sourceReferenceService.getAllSourceReferences(configuredTables)).thenReturn(Arrays.asList(
                sourceReference1, sourceReference2
        ));
        when(sourceReference1.getFullDatahubTableName()).thenReturn("table1");
        when(sourceReference2.getFullDatahubTableName()).thenReturn("table2");
        when(currentStateCountService.currentStateCountForTable(sparkSession, sourceReference1)).thenReturn(result1);
        when(currentStateCountService.currentStateCountForTable(sparkSession, sourceReference2)).thenReturn(result2);

        CurrentStateTotalCounts results = underTest.reconcileData(sparkSession).getCurrentStateTotalCounts();

        assertEquals(result1, results.get("table1"));
        assertEquals(result2, results.get("table2"));

        verify(currentStateCountService, times(1)).currentStateCountForTable(sparkSession, sourceReference1);
        verify(currentStateCountService, times(1)).currentStateCountForTable(sparkSession, sourceReference2);
    }

}