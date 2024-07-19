package uk.gov.justice.digital.datahub.model;

import lombok.Data;

@Data
public class OperationalDataStoreCredentials {
    private final String username;
    private final String password;
}
