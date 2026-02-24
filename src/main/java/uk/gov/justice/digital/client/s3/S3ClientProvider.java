package uk.gov.justice.digital.client.s3;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import uk.gov.justice.digital.client.ClientProvider;

import javax.inject.Singleton;

@Singleton
public class S3ClientProvider implements ClientProvider<S3Client> {
    @Override
    public S3Client getClient() {
        return S3Client.builder()
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .region(Region.EU_WEST_2)
                .defaultsMode(DefaultsMode.STANDARD)
                .build();
    }
}
