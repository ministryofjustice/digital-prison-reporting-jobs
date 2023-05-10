package uk.gov.justice.digital.domain.model;

import lombok.Data;

// TODO - review name
@Data
public class TableInfo {
    private final String prefix;
    private final String database;
    private final String schema;
    private final String table;
}

