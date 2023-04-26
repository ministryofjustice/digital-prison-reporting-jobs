package uk.gov.justice.digital.domain.model;

import lombok.Data;

@Data
public class TableInfo {
    protected String prefix;
    protected String schema;
    protected String table;

    protected TableInfo(final String prefix, final String schema, final String table) {
        this.prefix = prefix;
        this.schema = schema;
        this.table = table;
    }


    public static TableInfo create(final String prefix, final String schema, final String table) {
        return new TableInfo(prefix, schema, table);
    }

}

