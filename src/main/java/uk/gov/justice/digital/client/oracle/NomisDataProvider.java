package uk.gov.justice.digital.client.oracle;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.secretsmanager.SecretsManagerClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.NomisConnectionDetails;
import uk.gov.justice.digital.exception.OracleDataProviderException;
import uk.gov.justice.digital.provider.ConnectionPoolProvider;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Singleton
public class NomisDataProvider {

    private static final Logger logger = LoggerFactory.getLogger(NomisDataProvider.class);

    private static final String ORACLE_JDBC_DRIVER_NAME = "oracle.jdbc.driver.OracleDriver";

    // Used for accessing Nomis via JDBC
    private final DataSource dataSource;

    @Inject
    public NomisDataProvider(
            JobArguments jobArguments,
            ConnectionPoolProvider connectionPoolProvider,
            SecretsManagerClient secretsManagerClient
    ) {
        String secretName = jobArguments.getNomisConnectionDetailsSecretName();
        logger.info("Retrieving Nomis connection details from secretsmanager secret {}", secretName);
        NomisConnectionDetails nomisConnectionDetails = secretsManagerClient.getSecret(
                secretName,
                NomisConnectionDetails.class
        );
        logger.info("Creating DataSource for JDBC access to Nomis");
        dataSource = connectionPoolProvider.getConnectionPool(
                nomisConnectionDetails.getJdbcUrl(),
                ORACLE_JDBC_DRIVER_NAME,
                nomisConnectionDetails.getUser(),
                nomisConnectionDetails.getPassword()
        );
    }

    @SuppressWarnings("java:S2077")
    public long getTableCount(String tableName) {
        String query = "SELECT COUNT(1) FROM " + tableName;
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery(query);
                if (rs.next()) {
                    return rs.getLong(1);
                } else {
                    throw new OracleDataProviderException("No results returned while getting count of rows in table " + tableName);
                }
            }
        } catch (SQLException e) {
            throw new OracleDataProviderException("Exception while getting count of rows in table " + tableName, e);
        }
    }
}
