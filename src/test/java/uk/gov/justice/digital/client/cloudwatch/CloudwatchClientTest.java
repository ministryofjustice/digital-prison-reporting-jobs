package uk.gov.justice.digital.client.cloudwatch;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CloudwatchClientTest {

    @Mock
    private CloudwatchClientProvider cloudwatchClientProvider;
    @Mock
    private AmazonCloudWatch client;
    @Captor
    private ArgumentCaptor<PutMetricDataRequest> putMetricDataCaptor;

    private CloudwatchClient underTest;

    @BeforeEach
    void setup() {
        when(cloudwatchClientProvider.getClient()).thenReturn(client);
        underTest = new CloudwatchClient(cloudwatchClientProvider);
    }

    @Test
    void shouldPutMetrics() {
        String inputNamespace = "SomeNamespace";
        Collection<MetricDatum> inputMetricData = new HashSet<>();
        inputMetricData.add(new MetricDatum().withMetricName("SomeMetric"));

        underTest.putMetrics(inputNamespace, inputMetricData);

        verify(client, times(1)).putMetricData(putMetricDataCaptor.capture());

        PutMetricDataRequest putMetricDataRequest = putMetricDataCaptor.getValue();

        assertEquals(inputNamespace, putMetricDataRequest.getNamespace());
        assertEquals(inputMetricData, putMetricDataRequest.getMetricData());
    }
}