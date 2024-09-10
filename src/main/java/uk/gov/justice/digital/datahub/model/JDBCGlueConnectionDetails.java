package uk.gov.justice.digital.datahub.model;

import lombok.Data;

import java.util.Properties;

@Data
public class JDBCGlueConnectionDetails {
    private final String url;
    private final String jdbcDriverClassName;
    private final JDBCCredentials credentials;


    /**
     * Creates a Properties object that can be used with Spark JDBC reader/writer
     */
    public Properties toSparkJdbcProperties() {
        Properties jdbcProps = new Properties();
        jdbcProps.put("driver", getJdbcDriverClassName());
        jdbcProps.put("user", getCredentials().getUsername());
        jdbcProps.put("password", getCredentials().getPassword());
        return jdbcProps;
    }
}
