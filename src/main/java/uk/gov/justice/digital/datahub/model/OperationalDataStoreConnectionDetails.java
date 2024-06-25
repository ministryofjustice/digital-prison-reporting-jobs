package uk.gov.justice.digital.datahub.model;

import lombok.Data;

@Data
public class OperationalDataStoreConnectionDetails {
    private final String url;
    private final OperationalDataStoreCredentials credentials;
}
