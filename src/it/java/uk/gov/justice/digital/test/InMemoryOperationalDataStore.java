package uk.gov.justice.digital.test;

import org.h2.tools.Server;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreConnectionDetails;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreCredentials;
import uk.gov.justice.digital.provider.ConnectionPoolProvider;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;

import static java.lang.String.format;

public class InMemoryOperationalDataStore {
    private static final String DRIVER_CLASS_NAME = "org.h2.Driver";

    private Server h2Server;

    // use a unique database per instance of the in-memory datastore / test class
    private final String databaseName = "_" + UUID.randomUUID().toString().replaceAll("-", "_");

    public void start() throws SQLException {
        h2Server = Server.createPgServer().start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                h2Server.stop();
            } catch (Exception e) {
                // Squash on purpose since this is a best effort attempt to not leave around orphaned resources
            }
        }));
        try {
            // We shouldn't need to explicitly load the class like this, but we get intermittent test failures when
            // running integration tests in parallel due to "java.sql.SQLException: No suitable driver found" if we don't
            Class.forName(DRIVER_CLASS_NAME);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        if(h2Server != null) {
            h2Server.stop();
        }
    }

    public String getJdbcUrl() {
        return format("jdbc:h2:mem:%s;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1", databaseName);
    }

    public String getUsername() {
        return "sa";
    }

    public String getPassword() {
        return "";
    }

    public OperationalDataStoreCredentials getCredentials() {
        return new OperationalDataStoreCredentials(
                getUsername(), getPassword()
        );
    }

    public OperationalDataStoreConnectionDetails getConnectionDetails() {
        return new OperationalDataStoreConnectionDetails(
                getJdbcUrl(), getDriverClassName(), getCredentials()
        );
    }

    public String getDriverClassName() {
        return DRIVER_CLASS_NAME;
    }

    public Properties getJdbcProperties() {
        Properties jdbcProps = new Properties();
        jdbcProps.put("user", getUsername());
        jdbcProps.put("password", getPassword());
        return jdbcProps;
    }

    public Connection getJdbcConnection() throws SQLException {
        Properties jdbcProps = getJdbcProperties();
        Connection connection = DriverManager.getConnection(getJdbcUrl(), jdbcProps);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                connection.close();
            } catch (Exception e) {
                // Squash on purpose since this is a best effort attempt to not leave around orphaned resources
            }
        }));
        return connection;
    }

    public DataSource getConnectionPool() {
        return new ConnectionPoolProvider().getConnectionPool(
                getJdbcUrl(),
                getDriverClassName(),
                getUsername(),
                getPassword()
        );
    }
}
