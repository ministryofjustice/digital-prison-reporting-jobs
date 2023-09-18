package uk.gov.justice.digital.client.secretsmanager;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.exception.SecretsManagerClientException;

import javax.inject.Singleton;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

@Singleton
public class SecretsManagerClient implements Serializable {

    private static final long serialVersionUID = -4487191414062715636L;

    private static final Logger logger = LoggerFactory.getLogger(SecretsManagerClient.class);

    private final SecretsManagerClientProvider secretsClientProvider;

    // AWSSecretsManager object requires special handling for Spark checkpointing since it is not serializable.
    // Transient ensures Java does not attempt to serialize it.
    // Volatile ensures it keeps the 'final' concurrency initialization guarantee.
    private transient volatile AWSSecretsManager secretsClient;

    @Inject
    public SecretsManagerClient(SecretsManagerClientProvider secretsClientProvider) {
        this.secretsClientProvider = secretsClientProvider;
        this.secretsClient = secretsClientProvider.getClient();
    }

    public <T> T getSecret(String secretId, Class<T> type) throws SecretsManagerClientException {
        logger.info("Getting secret {}", secretId);
        try {
            val request = new GetSecretValueRequest().withSecretId(secretId);
            val secretValue = secretsClient.getSecretValue(request).getSecretString();
            return new ObjectMapper().readValue(secretValue, type);
        } catch (Exception ex) {
            val errorMessage = "Secret " + secretId + " is missing";
            logger.error(errorMessage, ex);
            throw new SecretsManagerClientException(errorMessage);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.secretsClient = this.secretsClientProvider.getClient();
    }

}
