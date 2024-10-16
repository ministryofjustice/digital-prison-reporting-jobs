package uk.gov.justice.digital.client.cloudwatch;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

@Singleton
public class CloudwatchClient {

    private static final Logger logger = LoggerFactory.getLogger(CloudwatchClient.class);

    private final AmazonCloudWatch client;

    @Inject
    public CloudwatchClient(CloudwatchClientProvider cloudwatchClientProvider) {
        this.client = cloudwatchClientProvider.getClient();
    }

    public void putMetrics(String namespace, Collection<MetricDatum> metricData) {
        logger.debug("Putting metrics to namespace {}", namespace);
        PutMetricDataRequest putMetricDataRequest = new PutMetricDataRequest();
        putMetricDataRequest.setNamespace(namespace);
        putMetricDataRequest.setMetricData(metricData);
        client.putMetricData(putMetricDataRequest);
        logger.debug("Finished putting metrics to namespace {}", namespace);
    }
}
