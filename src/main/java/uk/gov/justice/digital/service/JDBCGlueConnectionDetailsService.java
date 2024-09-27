package uk.gov.justice.digital.service;


import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.glue.GlueClient;
import uk.gov.justice.digital.client.secretsmanager.SecretsManagerClient;
import uk.gov.justice.digital.datahub.model.JDBCCredentials;
import uk.gov.justice.digital.datahub.model.JDBCGlueConnectionDetails;
import uk.gov.justice.digital.exception.JDBCGlueConnectionDetailsException;

import java.util.Map;

/**
 * Responsible for retrieving JDBC details from AWS Glue Connections.
 */
@Singleton
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
        String jdbcUrl = connectionProperties.get("JDBC_CONNECTION_URL");
        if (jdbcUrl == null) {
            throw new JDBCGlueConnectionDetailsException("JDBC url was null");
        }
        String jdbcDriverClassName = connectionProperties.get("JDBC_DRIVER_CLASS_NAME");
        if (jdbcDriverClassName == null) {
            throw new JDBCGlueConnectionDetailsException("JDBC driver class name was null");
        }
        logger.debug("Using connection URL {}", jdbcUrl);
        String secretId = connectionProperties.get("SECRET_ID");
        JDBCCredentials credentials = secretsManagerClient.getSecret(secretId, JDBCCredentials.class);
        if (credentials.getUsername() == null || credentials.getPassword() == null) {
            throw new JDBCGlueConnectionDetailsException("Username or password was null");
        }
        JDBCGlueConnectionDetails connectionDetails = new JDBCGlueConnectionDetails(jdbcUrl, jdbcDriverClassName, credentials);
        logger.debug("Finished getting connection details for connection {} in {}ms", connectionName, System.currentTimeMillis() - startTime);
        return connectionDetails;
    }
}
