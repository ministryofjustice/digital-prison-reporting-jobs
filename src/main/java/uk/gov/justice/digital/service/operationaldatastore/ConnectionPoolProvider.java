package uk.gov.justice.digital.service.operationaldatastore;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.inject.Singleton;

import javax.sql.DataSource;

@Singleton
public class ConnectionPoolProvider {

    private static final int maxHikariPoolSize = 10;

    DataSource getConnectionPool(
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
        hikariConfig.setMaximumPoolSize(maxHikariPoolSize);
        return new HikariDataSource(hikariConfig);
    }
}
