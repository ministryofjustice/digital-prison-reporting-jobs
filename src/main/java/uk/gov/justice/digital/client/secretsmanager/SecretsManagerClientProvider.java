package uk.gov.justice.digital.client.secretsmanager;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import uk.gov.justice.digital.client.ClientProvider;

import javax.inject.Singleton;

@Singleton
public class SecretsManagerClientProvider implements ClientProvider<AWSSecretsManager> {
    @Override
    public AWSSecretsManager getClient() {
        return AWSSecretsManagerClientBuilder.standard().build();
    }
}