package uk.gov.justice.digital.client.dms;

import jakarta.inject.Singleton;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.databasemigration.DatabaseMigrationClient;
import uk.gov.justice.digital.client.ClientProvider;

@Singleton
public class DmsClientProvider implements ClientProvider<DatabaseMigrationClient> {

    @Override
    public DatabaseMigrationClient getClient() {
        return DatabaseMigrationClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .region(Region.EU_WEST_2)
                .defaultsMode(DefaultsMode.STANDARD)
                .build();
    }

}
