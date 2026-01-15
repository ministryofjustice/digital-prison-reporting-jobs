package uk.gov.justice.digital.service.datareconciliation;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.cloudwatch.CloudwatchClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

import java.util.Collection;
import java.util.List;

import static com.amazonaws.services.cloudwatch.model.StandardUnit.Count;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CloudwatchMetricReportingServiceTest {

    private static final String DOMAIN = "some domain";
    private static final String NAMESPACE = "SomeNamespace";
    @Mock
    private JobArguments jobArguments;
    @Mock
    private CloudwatchClient cloudwatchClient;
    @Mock
    private DataReconciliationResults dataReconciliationResults;
    @Captor
    private ArgumentCaptor<Collection<MetricDatum>> metricDatumCaptor;

    private CloudwatchMetricReportingService underTest;

    @BeforeEach
    void setUp() {
        underTest = new CloudwatchMetricReportingService(jobArguments, cloudwatchClient);
    }

    @Test
    void reportMetricsShouldPutMetrics() {

        when(jobArguments.getConfigKey()).thenReturn(DOMAIN);
        when(jobArguments.getReconciliationCloudwatchMetricsNamespace()).thenReturn(NAMESPACE);
        when(dataReconciliationResults.numReconciliationChecksFailing()).thenReturn(2L);

        underTest.reportMetrics(dataReconciliationResults);

        verify(cloudwatchClient, times(1)).putMetrics(eq(NAMESPACE), metricDatumCaptor.capture());

        Collection<MetricDatum> sentMetrics = metricDatumCaptor.getValue();

        assertEquals(1, sentMetrics.size());
        MetricDatum datum = sentMetrics.iterator().next();

        assertEquals("FailedReconciliationChecks", datum.getMetricName());
        assertEquals(2L, datum.getValue());
        assertEquals(Count.toString(), datum.getUnit());
        List<Dimension> dimensions = datum.getDimensions();
        assertEquals(1, dimensions.size());
        Dimension dimension = dimensions.get(0);
        assertEquals("InputDomain", dimension.getName());
        assertEquals(DOMAIN, dimension.getValue());
    }
}
