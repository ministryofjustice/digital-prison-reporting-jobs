package uk.gov.justice.digital.test;

import org.h2.tools.Server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;

import static java.lang.String.format;

public class InMemoryOperationalDataStore {

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

    public String getDriverClassName() {
        return "org.h2.Driver";
    }

    public Connection getJdbcConnection() throws SQLException {
        Properties jdbcProps = new Properties();
        jdbcProps.put("user", getUsername());
        jdbcProps.put("password", getPassword());
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
}
