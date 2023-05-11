package uk.gov.justice.digital.domain.model;

import lombok.Data;

@Data
public class TableIdentifier {
    private final String basePath;  // Base path on filesystem where table data is located
    private final String database;  // Database name
    private final String schema;    // Schema name
    private final String table;     // Table name
}

