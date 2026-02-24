package uk.gov.justice.digital.client.cloudwatch;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import jakarta.inject.Singleton;
import uk.gov.justice.digital.client.ClientProvider;

@Singleton
public class CloudwatchClientProvider implements ClientProvider<CloudWatchClient> {
    @Override
    public CloudWatchClient getClient() {
        return CloudWatchClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .region(Region.EU_WEST_2)
                .defaultsMode(DefaultsMode.STANDARD)
                .build();
    }
}