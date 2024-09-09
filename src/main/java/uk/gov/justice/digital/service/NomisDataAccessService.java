package uk.gov.justice.digital.service;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.JDBCGlueConnectionDetails;
import uk.gov.justice.digital.exception.NomisDataAccessException;
import uk.gov.justice.digital.provider.ConnectionPoolProvider;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Responsible for access to data in NOMIS.
 */
@Singleton
public class NomisDataAccessService {

    private static final Logger logger = LoggerFactory.getLogger(NomisDataAccessService.class);

    // Used for accessing Nomis via JDBC
    private final DataSource dataSource;

    @Inject
    public NomisDataAccessService(
            JobArguments jobArguments,
            ConnectionPoolProvider connectionPoolProvider,
            JDBCGlueConnectionDetailsService connectionDetailsService
    ) {
        logger.debug("Retrieving connection details for NOMIS");
        String connectionName = jobArguments.getNomisGlueConnectionName();
        JDBCGlueConnectionDetails connectionDetails = connectionDetailsService.getConnectionDetails(connectionName);
        dataSource = connectionPoolProvider.getConnectionPool(
                connectionDetails.getUrl(),
                connectionDetails.getJdbcDriverClassName(),
                connectionDetails.getCredentials().getUsername(),
                connectionDetails.getCredentials().getPassword()
        );
        logger.debug("Finished retrieving connection details for NOMIS");
    }

    @SuppressWarnings("java:S2077")
    public long getTableRowCount(String tableName) {
        String query = "SELECT COUNT(1) FROM " + tableName;
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery(query);
                if (rs.next()) {
                    return rs.getLong(1);
                } else {
                    throw new NomisDataAccessException("No results returned while getting count of rows in table " + tableName);
                }
            }
        } catch (SQLException e) {
            throw new NomisDataAccessException("Exception while getting count of rows in table " + tableName, e);
        }
    }
}
