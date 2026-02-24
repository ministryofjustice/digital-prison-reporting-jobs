package uk.gov.justice.digital.client.secretsmanager;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import uk.gov.justice.digital.client.ClientProvider;

import javax.inject.Singleton;

@Singleton
public class SecretsManagerClientProvider implements ClientProvider<SecretsManagerClient> {
    @Override
    public SecretsManagerClient getClient() {
        return SecretsManagerClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .region(Region.EU_WEST_2)
                .defaultsMode(DefaultsMode.STANDARD)
                .build();
    }
}
