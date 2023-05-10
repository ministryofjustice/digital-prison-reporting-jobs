package uk.gov.justice.digital.domain.model;

import lombok.Data;

@Data
public class HiveTableIdentifier {
    private final String prefix;    // TODO - This is a filesystem prefix - could (should?) be a Path
    private final String database;
    private final String schema;
    private final String table;
}

