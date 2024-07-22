package uk.gov.justice.digital.datahub.model;

import lombok.Data;

/**
 * Represents a DataHub table under management by the Operational DataStore.
 * It corresponds to an entry in the Operational DataStore DataHub Managed Tables table.
 * This is used by the DataHub jobs to decide whether tables should be written to the Operational DataStore.
 */
@Data
public class DataHubOperationalDataStoreManagedTable {
    private final String source;
    private final String tableName;
}
