package uk.gov.justice.digital.client.cloudwatch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;

import java.util.Collection;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CloudwatchClientTest {

    @Mock
    private CloudwatchClientProvider cloudwatchClientProvider;
    @Mock
    private CloudWatchClient client;
    @Captor
    private ArgumentCaptor<PutMetricDataRequest> putMetricDataCaptor;

    private DefaultCloudwatchClient underTest;

    @BeforeEach
    void setup() {
        when(cloudwatchClientProvider.getClient()).thenReturn(client);
        underTest = new DefaultCloudwatchClient(cloudwatchClientProvider);
    }

    @Test
    void shouldPutMetrics() {
        String inputNamespace = "SomeNamespace";
        Collection<MetricDatum> inputMetricData = new HashSet<>();
        inputMetricData.add(MetricDatum.builder().metricName("SomeMetric").build());

        underTest.putMetrics(inputNamespace, inputMetricData);

        verify(client, times(1)).putMetricData(putMetricDataCaptor.capture());

        PutMetricDataRequest putMetricDataRequest = putMetricDataCaptor.getValue();

        assertEquals(inputNamespace, putMetricDataRequest.namespace());
        assertIterableEquals(inputMetricData, putMetricDataRequest.metricData());
    }
}