package uk.gov.justice.digital.domains.model;

public class TableTuple {
    private String schema;
    private String table;

    private String originalSchema;
    private String originalTable;

    public TableTuple() {

    }

    public TableTuple(final String source) {
        if(source != null && source.contains(".")) {
            this.schema = source.split("\\.")[0];
            this.table = source.split("\\.")[1];
        }
        this.originalSchema = schema;
        this.originalTable = table;
    }

    public TableTuple(final String schema, final String table) {
        this.schema = schema;
        this.table = table;

        this.originalSchema = schema;
        this.originalTable = table;
    }

    public TableTuple(final String schema, final String table, final String origSchema, final String origTable) {
        this.schema = schema;
        this.table = table;

        this.originalSchema = origSchema;
        this.originalTable = origTable;
    }

    public String getSchema() {
        return schema;
    }
    public void setSchema(String schema) {
        this.schema = schema;
    }
    public String getTable() {
        return table;
    }
    public void setTable(String table) {
        this.table = table;
    }

    public String asString() {
        return schema + "." + table;
    }

    public String asString(final String sep) {
        return schema + sep + table;
    }

    public String getOriginalSchema() {
        return originalSchema;
    }

    public void setOriginalSchema(String originalSchema) {
        this.originalSchema = originalSchema;
    }

    public String getOriginalTable() {
        return originalTable;
    }

    public void setOriginalTable(String originalTable) {
        this.originalTable = originalTable;
    }

}
