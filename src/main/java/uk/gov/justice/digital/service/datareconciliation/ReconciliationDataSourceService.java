package uk.gov.justice.digital.service.datareconciliation;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.JDBCGlueConnectionDetails;
import uk.gov.justice.digital.exception.ReconciliationDataSourceException;
import uk.gov.justice.digital.provider.ConnectionPoolProvider;
import uk.gov.justice.digital.service.JDBCGlueConnectionDetailsService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Responsible for access to data in a data source, such as NOMIS or a DPS database.
 */
@Singleton
public class ReconciliationDataSourceService {

    private static final Logger logger = LoggerFactory.getLogger(ReconciliationDataSourceService.class);

    private final String sourceSchemaName;
    private final boolean shouldUppercaseTableNames;
    // Used for accessing the datasource via JDBC
    private final DataSource dataSource;

    @Inject
    public ReconciliationDataSourceService(
            JobArguments jobArguments,
            ConnectionPoolProvider connectionPoolProvider,
            JDBCGlueConnectionDetailsService connectionDetailsService
    ) {
        this.sourceSchemaName = jobArguments.getReconciliationDataSourceSourceSchemaName();
        this.shouldUppercaseTableNames = jobArguments.shouldReconciliationDataSourceTableNamesBeUpperCase();
        logger.debug(
                "Reconciliation data source will use source schema {} and {} case table names",
                sourceSchemaName, shouldUppercaseTableNames ? "upper" : "lower"
        );
        String connectionName = jobArguments.getReconciliationDataSourceGlueConnectionName();
        logger.debug("Retrieving connection details for {}", connectionName);
        JDBCGlueConnectionDetails connectionDetails = connectionDetailsService.getConnectionDetails(connectionName);
        this.dataSource = connectionPoolProvider.getConnectionPool(
                connectionDetails.getUrl(),
                connectionDetails.getJdbcDriverClassName(),
                connectionDetails.getCredentials().getUsername(),
                connectionDetails.getCredentials().getPassword()
        );
        logger.debug("Finished retrieving connection details for {}", connectionName);
    }

    @SuppressWarnings("java:S2077")
    public long getTableRowCount(String tableName) {
        String fullTableName = sourceSchemaName + "." + tableName;
        if (shouldUppercaseTableNames) {
            fullTableName = fullTableName.toUpperCase();
        }
        String query = "SELECT COUNT(1) FROM " + fullTableName;
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery(query);
                if (rs.next()) {
                    return rs.getLong(1);
                } else {
                    throw new ReconciliationDataSourceException("No results returned while getting count of rows in table " + fullTableName);
                }
            }
        } catch (SQLException e) {
            throw new ReconciliationDataSourceException("Exception while getting count of rows in table " + fullTableName, e);
        }
    }
}
