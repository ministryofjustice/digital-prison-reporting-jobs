package uk.gov.justice.digital.service.datareconciliation;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class DisabledReconciliationMetricReportingServiceTest {

    @Mock
    private DataReconciliationResults dataReconciliationResults;

    @Test
    void reportMetricsShouldDoNothing() {
        DisabledReconciliationMetricReportingService underTest = new DisabledReconciliationMetricReportingService();
        underTest.reportMetrics(dataReconciliationResults);

        verifyNoInteractions(dataReconciliationResults);
    }

}