package uk.gov.justice.digital.domain.model;

import lombok.Data;

// TODO - review name
@Data
public class TableInfo {
    private String prefix = null;
    private String schema = null;
    private String table = null;
    private String database = null;

    // TODO - remove this - @Data provides this
    protected TableInfo(final String prefix, final String database, final String schema, final String table) {
        this.prefix = prefix;
        this.database = database;
        this.schema = schema;
        this.table = table;
    }


    public TableInfo() {
        // Empty constructor
    }

    public static TableInfo create(final String prefix, final String database, final String schema, final String table) {
        return new TableInfo(prefix, database, schema, table);
    }

}

