package uk.gov.justice.digital.provider;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.inject.Singleton;

import javax.sql.DataSource;
import java.time.Duration;

@Singleton
public class ConnectionPoolProvider {

    private static final int MAX_HIKARI_POOL_SIZE = 10;
    // Connections are only opened when needed and closed again once they've been idle for this long,
    // so long-running jobs don't hold connections they aren't using.
    private static final int MIN_IDLE_CONNECTIONS = 0;
    private static final long IDLE_TIMEOUT_MILLIS = Duration.ofSeconds(30).toMillis();

    public DataSource getConnectionPool(
            String jdbcUrl,
            String jdbcDriverClassName,
            String username,
            String password
    ) {
        return new HikariDataSource(createConfig(jdbcUrl, jdbcDriverClassName, username, password));
    }

    static HikariConfig createConfig(
            String jdbcUrl,
            String jdbcDriverClassName,
            String username,
            String password
    ) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setDriverClassName(jdbcDriverClassName);
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.setMaximumPoolSize(MAX_HIKARI_POOL_SIZE);
        hikariConfig.setMinimumIdle(MIN_IDLE_CONNECTIONS);
        hikariConfig.setIdleTimeout(IDLE_TIMEOUT_MILLIS);
        return hikariConfig;
    }
}
