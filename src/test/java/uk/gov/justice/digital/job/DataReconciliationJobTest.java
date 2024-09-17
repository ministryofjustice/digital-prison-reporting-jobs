package uk.gov.justice.digital.job;

import com.ginsberg.junit.exit.ExpectSystemExitWithStatus;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.datareconciliation.DataReconciliationService;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataReconciliationJobTest {
    @Mock
    private JobProperties properties;
    @Mock
    private JobArguments jobArguments;
    @Mock
    private DataReconciliationService dataReconciliationService;
    @Mock
    private SparkSession sparkSession;
    @Mock
    private DataReconciliationResults results;

    private DataReconciliationJob underTest;

    @BeforeEach
    void setUp() {
        underTest = new DataReconciliationJob(properties, jobArguments, new SparkSessionProvider(), dataReconciliationService);
    }

    @Test
    void runJobShouldReconcileData() {
        when(dataReconciliationService.reconcileData(sparkSession)).thenReturn(results);
        when(results.isSuccess()).thenReturn(true);

        underTest.runJob(sparkSession);

        verify(dataReconciliationService, times(1)).reconcileData(sparkSession);
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    @SuppressWarnings("java:S2699")
    void runJobShouldSystemExitWithErrorExitCodeWhenResultIsFailure() {
        when(dataReconciliationService.reconcileData(sparkSession)).thenReturn(results);
        when(results.isSuccess()).thenReturn(false);

        underTest.runJob(sparkSession);
    }
}