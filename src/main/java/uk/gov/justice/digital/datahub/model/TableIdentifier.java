package uk.gov.justice.digital.datahub.model;

import lombok.Data;

import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;

@Data
public class TableIdentifier {
    private final String basePath;  // Base path on filesystem where table data is located
    private final String database;  // Database name
    private final String schema;    // Schema name
    private final String table;     // Table name

    public String toPath() {
        return createValidatedPath(basePath, schema, table);
    }
}

