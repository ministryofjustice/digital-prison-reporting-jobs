package uk.gov.justice.digital.client.secretsmanager;

import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.exception.SecretsManagerClientException;

import javax.inject.Singleton;

@Singleton
public class SecretsClient {

    private static final Logger logger = LoggerFactory.getLogger(SecretsClient.class);

    private final SecretsManagerClient secretsManagerClient;

    @Inject
    public SecretsClient(SecretsManagerClientProvider secretsClient) {
        this.secretsManagerClient = secretsClient.getClient();
    }

    public <T> T getSecret(String secretId, Class<T> type) throws SecretsManagerClientException {
        logger.info("Getting secret {}", secretId);
        try {
            val request = GetSecretValueRequest.builder().secretId(secretId).build();
            val secretValue = secretsManagerClient.getSecretValue(request).secretString();
            return new ObjectMapper().readValue(secretValue, type);
        } catch (Exception ex) {
            val errorMessage = "Secret " + secretId + " is missing";
            logger.error(errorMessage, ex);
            throw new SecretsManagerClientException(errorMessage);
        }
    }

}
