package uk.gov.justice.digital.provider;

import com.zaxxer.hikari.HikariConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConnectionPoolProviderTest {

    @Test
    void shouldNotKeepIdleConnectionsOpen() {
        HikariConfig config = ConnectionPoolProvider.createConfig(
                "jdbc:postgresql://localhost:5432/db", "org.postgresql.Driver", "username", "password"
        );
        assertEquals(0, config.getMinimumIdle());
        assertEquals(30_000L, config.getIdleTimeout());
        assertEquals(10, config.getMaximumPoolSize());
    }
}
