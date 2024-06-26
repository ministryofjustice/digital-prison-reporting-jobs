package uk.gov.justice.digital.service.operationaldatastore;


import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import uk.gov.justice.digital.client.glue.GlueClient;
import uk.gov.justice.digital.client.secretsmanager.SecretsManagerClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreConnectionDetails;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreCredentials;

import java.util.Map;

/**
 * Responsible for retrieving details for connecting to the Operational DataStore.
 */
@Singleton
public class OperationalDataStoreConnectionDetailsService {

    private final GlueClient glueClient;
    private final SecretsManagerClient secretsManagerClient;
    private final JobArguments jobArguments;

    @Inject
    public OperationalDataStoreConnectionDetailsService(
            GlueClient glueClient,
            SecretsManagerClient secretsManagerClient,
            JobArguments jobArguments
    ) {
        this.glueClient = glueClient;
        this.secretsManagerClient = secretsManagerClient;
        this.jobArguments = jobArguments;
    }

    public OperationalDataStoreConnectionDetails getConnectionDetails() {
        String connectionName = jobArguments.getOperationalDataStoreGlueConnectionName();
        com.amazonaws.services.glue.model.Connection connection = glueClient.getConnection(connectionName);
        Map<String, String> connectionProperties = connection.getConnectionProperties();
        String url = connectionProperties.get("JDBC_CONNECTION_URL");
        String secretId = connectionProperties.get("SECRET_ID");
        OperationalDataStoreCredentials credentials = secretsManagerClient.getSecret(secretId, OperationalDataStoreCredentials.class);
        return new OperationalDataStoreConnectionDetails(url, credentials);
    }
}