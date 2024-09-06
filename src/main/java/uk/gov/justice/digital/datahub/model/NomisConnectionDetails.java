package uk.gov.justice.digital.datahub.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import static java.lang.String.format;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class NomisConnectionDetails {
    private String user;
    private String password;
    private String endpoint;
    @JsonProperty("db_name")
    private String dbName;
    private int port;


    public String getJdbcUrl() {
        return format("jdbc:oracle:thin:@%s:%d:%s", endpoint, port, dbName);
    }
}
