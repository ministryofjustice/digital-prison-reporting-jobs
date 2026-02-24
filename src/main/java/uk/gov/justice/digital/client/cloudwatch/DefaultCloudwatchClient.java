package uk.gov.justice.digital.client.cloudwatch;

import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

@Singleton
public class DefaultCloudwatchClient {

    private static final Logger logger = LoggerFactory.getLogger(DefaultCloudwatchClient.class);

    private final CloudWatchClient client;

    @Inject
    public DefaultCloudwatchClient(CloudwatchClientProvider cloudwatchClientProvider) {
        this.client = cloudwatchClientProvider.getClient();
    }

    public void putMetrics(String namespace, Collection<MetricDatum> metricData) {
        logger.debug("Putting metrics to namespace {}", namespace);
        PutMetricDataRequest putMetricDataRequest = PutMetricDataRequest.builder()
                .namespace(namespace)
                .metricData(metricData)
                .build();
        client.putMetricData(putMetricDataRequest);
        logger.debug("Finished putting metrics to namespace {}", namespace);
    }
}