package uk.gov.justice.digital.service;


import io.micronaut.context.annotation.Prototype;
import jakarta.inject.Inject;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.glue.GlueClient;
import uk.gov.justice.digital.client.secretsmanager.SecretsManagerClient;
import uk.gov.justice.digital.datahub.model.JDBCCredentials;
import uk.gov.justice.digital.datahub.model.JDBCGlueConnectionDetails;

import java.util.Map;

/**
 * Responsible for retrieving JDBC details from AWS Glue Connections.
 */
@Prototype
public class JDBCGlueConnectionDetailsService {

    private static final Logger logger = LoggerFactory.getLogger(JDBCGlueConnectionDetailsService.class);

    private final GlueClient glueClient;
    private final SecretsManagerClient secretsManagerClient;

    @Inject
    public JDBCGlueConnectionDetailsService(
            GlueClient glueClient,
            SecretsManagerClient secretsManagerClient
    ) {
        this.glueClient = glueClient;
        this.secretsManagerClient = secretsManagerClient;
    }

    public JDBCGlueConnectionDetails getConnectionDetails(String connectionName) {
        val startTime = System.currentTimeMillis();
        logger.debug("Getting connection details for connection {}", connectionName);
        com.amazonaws.services.glue.model.Connection connection = glueClient.getConnection(connectionName);
        Map<String, String> connectionProperties = connection.getConnectionProperties();
        String url = connectionProperties.get("JDBC_CONNECTION_URL");
        String jdbcDriverClassName = connectionProperties.get("JDBC_DRIVER_CLASS_NAME");
        logger.debug("Using connection URL {}", url);
        String secretId = connectionProperties.get("SECRET_ID");
        logger.debug("Retrieving credentials from secret ID {}", secretId);
        JDBCCredentials credentials = secretsManagerClient.getSecret(secretId, JDBCCredentials.class);
        logger.debug("Will use user {}", credentials.getUsername());
        JDBCGlueConnectionDetails connectionDetails = new JDBCGlueConnectionDetails(url, jdbcDriverClassName, credentials);
        logger.debug("Finished getting connection details for connection {} in {}ms", connectionName, System.currentTimeMillis() - startTime);
        return connectionDetails;
    }
}
