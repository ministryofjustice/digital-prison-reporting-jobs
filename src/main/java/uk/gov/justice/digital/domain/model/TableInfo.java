package uk.gov.justice.digital.domain.model;

import lombok.Data;

// TODO - review name
@Data
public class TableInfo {
    private final String prefix;
    private final String schema;
    private final String table;
    private final String database;

    // TODO - remove this - @Data provides this
    protected TableInfo(final String prefix, final String database, final String schema, final String table) {
        this.prefix = prefix;
        this.database = database;
        this.schema = schema;
        this.table = table;
    }

    // TODO - remove this - not necessary
    public static TableInfo create(final String prefix, final String database, final String schema, final String table) {
        return new TableInfo(prefix, database, schema, table);
    }

}

