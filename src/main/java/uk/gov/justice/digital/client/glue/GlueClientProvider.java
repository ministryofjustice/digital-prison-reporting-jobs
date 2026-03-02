package uk.gov.justice.digital.client.glue;

import jakarta.inject.Singleton;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import uk.gov.justice.digital.client.ClientProvider;

@Singleton
public class GlueClientProvider implements ClientProvider<GlueClient> {

    @Override
    public GlueClient getClient() {
        return GlueClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .region(Region.EU_WEST_2)
                .defaultsMode(DefaultsMode.STANDARD)
                .build();
    }

}
