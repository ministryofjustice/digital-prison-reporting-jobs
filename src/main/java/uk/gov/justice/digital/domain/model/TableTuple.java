package uk.gov.justice.digital.domain.model;

import lombok.Data;
import java.util.regex.PatternSyntaxException;

@Data
public class TableTuple {
    private String schema;
    private String table;

    private final String originalSchema;
    private final String originalTable;


    public TableTuple(final String source) throws PatternSyntaxException {
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


    public String asString() {
        return schema + "." + table;
    }


}
