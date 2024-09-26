package uk.gov.justice.digital.provider;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micronaut.context.annotation.Prototype;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

@Prototype
public class ConnectionPoolProvider {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionPoolProvider.class);

    private static final int MAX_HIKARI_POOL_SIZE = 10;

    public DataSource getConnectionPool(
            String jdbcUrl,
            String jdbcDriverClassName,
            String username,
            String password
    ) {
        logger.debug("Setting up connection pool with user: {}, driver class {}, jdbcUrl: {}",
                username, jdbcDriverClassName, jdbcUrl);
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setDriverClassName(jdbcDriverClassName);
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.setMaximumPoolSize(MAX_HIKARI_POOL_SIZE);
        return new HikariDataSource(hikariConfig);
    }
}
