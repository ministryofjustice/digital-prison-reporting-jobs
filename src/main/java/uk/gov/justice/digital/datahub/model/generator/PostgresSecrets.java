package uk.gov.justice.digital.datahub.model.generator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@NoArgsConstructor
@AllArgsConstructor
@Data
public class PostgresSecrets {
    private String dbName;
    private String heartbeatEndpoint;
    private String password;
    private String port;
    private String username;

    public String getDatabaseName() {
        return dbName;
    }

    public String getHeartbeatEndpoint() {
        return heartbeatEndpoint;
    }

    public String getPassword() {
        return password;
    }

    public String getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getJdbcUrl() {
        return "jdbc:postgresql://" + getHeartbeatEndpoint() + ":" + getPort() + "/" + getDatabaseName();
    }
}
