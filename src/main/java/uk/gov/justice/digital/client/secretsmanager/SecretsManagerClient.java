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
import java.io.Serializable;

@Singleton
public class SecretsManagerClient implements Serializable {

    private static final long serialVersionUID = -4487191414062715636L;

    private static final Logger logger = LoggerFactory.getLogger(SecretsManagerClient.class);

    private final AWSSecretsManager secretsClient;

    @Inject
    public SecretsManagerClient(SecretsManagerClientProvider secretsClient) {
        this.secretsClient = secretsClient.getClient();
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

}
