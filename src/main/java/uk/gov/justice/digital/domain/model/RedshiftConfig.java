package uk.gov.justice.digital.domain.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class RedshiftConfig {
    private String host;
    private String password;
    private String username;
}
