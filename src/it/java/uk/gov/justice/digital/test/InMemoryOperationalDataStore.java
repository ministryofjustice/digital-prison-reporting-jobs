package uk.gov.justice.digital.test;

import org.h2.tools.Server;

import java.sql.SQLException;

public class InMemoryOperationalDataStore {

    private Server h2Server;

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

    public void stop() throws SQLException {
        if(h2Server != null) {
            h2Server.stop();
        }
    }

    public String getJdbcUrl() {
        return "jdbc:h2:~/test;MODE=PostgreSQL";
    }

    public String getUsername() {
        return "sa";
    }

    public String getPassword() {
        return "";
    }
}
