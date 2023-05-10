package uk.gov.justice.digital.domain.model;

import lombok.Data;

@Data
public class HiveTableIdentifier {
    private final String basePath;  // Base path on filesystem where table data is located
    private final String database;  // Hive database name
    private final String schema;    // Schema name
    private final String table;     // Hive table name
}

