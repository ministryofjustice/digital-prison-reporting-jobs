package uk.gov.justice.digital.client.secretsmanager;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import uk.gov.justice.digital.client.ClientProvider;

import javax.inject.Singleton;
import java.io.Serializable;

@Singleton
public class SecretsManagerClientProvider implements ClientProvider<AWSSecretsManager>, Serializable {

    private static final long serialVersionUID = -2424390721463286554L;
    @Override
    public AWSSecretsManager getClient() {
        return AWSSecretsManagerClientBuilder.standard().build();
    }
}
