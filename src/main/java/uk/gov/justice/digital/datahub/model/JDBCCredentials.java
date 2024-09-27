package uk.gov.justice.digital.datahub.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Data
public class JDBCCredentials {
    @JsonProperty(value = "username", required = true)
    private String username;
    @JsonProperty(value = "password", required = true)
    private String password;
}
