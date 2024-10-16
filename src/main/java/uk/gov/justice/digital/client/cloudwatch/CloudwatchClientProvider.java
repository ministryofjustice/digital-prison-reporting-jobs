package uk.gov.justice.digital.client.cloudwatch;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import jakarta.inject.Singleton;
import uk.gov.justice.digital.client.ClientProvider;

@Singleton
public class CloudwatchClientProvider implements ClientProvider<AmazonCloudWatch> {
    @Override
    public AmazonCloudWatch getClient() {
        return AmazonCloudWatchClientBuilder.defaultClient();
    }
}
