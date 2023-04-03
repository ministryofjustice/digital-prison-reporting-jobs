package uk.gov.justice.digital.domains.model;

public class TableInfo {
    protected String prefix;
    protected String schema;
    protected String table;

    protected TableInfo(final String prefix, final String schema, final String table) {
        this.prefix = prefix;
        this.schema = schema;
        this.table = table;
    }

    public static TableInfo create(final String prefix, final String source) {
        String[] parts = source.split("\\.");
        if(parts == null || parts.length == 0) {
            return new TableInfo(prefix, "","");
        }
        if(parts.length == 1) {
            return new TableInfo(prefix, "", parts[0]);
        }

        return new TableInfo(prefix, parts[0], parts[1]);
    }

    public static TableInfo create(final String prefix, final String schema, final String table) {
        return new TableInfo(prefix, schema, table);
    }

    public String getPrefix() {
        return prefix;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

}

