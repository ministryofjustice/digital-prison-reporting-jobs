package uk.gov.justice.digital.datahub.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

/**
 * Data Transfer Object for the Nomis connection details stored in AWS Secrets Manager.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class NomisConnectionDetails {
    private String user;
    private String password;
}
