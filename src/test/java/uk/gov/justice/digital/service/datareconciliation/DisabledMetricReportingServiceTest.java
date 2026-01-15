package uk.gov.justice.digital.service.datareconciliation;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;
import uk.gov.justice.digital.service.metrics.DisabledMetricReportingService;

import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class DisabledMetricReportingServiceTest {

    @Mock
    private DataReconciliationResults dataReconciliationResults;

    @Test
    void reportDataReconciliationResultsShouldDoNothing() {
        DisabledMetricReportingService underTest = new DisabledMetricReportingService();
        underTest.reportDataReconciliationResults(dataReconciliationResults);

        verifyNoInteractions(dataReconciliationResults);
    }

}
