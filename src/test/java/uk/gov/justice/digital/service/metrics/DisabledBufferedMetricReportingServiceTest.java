package uk.gov.justice.digital.service.metrics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class DisabledBufferedMetricReportingServiceTest {

    @Mock
    private DataReconciliationResults dataReconciliationResults;

    @Test
    void bufferDataReconciliationResultsShouldDoNothing() {
        DisabledBufferedMetricReportingService underTest = new DisabledBufferedMetricReportingService();
        underTest.bufferDataReconciliationResults(dataReconciliationResults);

        verifyNoInteractions(dataReconciliationResults);
    }

}
