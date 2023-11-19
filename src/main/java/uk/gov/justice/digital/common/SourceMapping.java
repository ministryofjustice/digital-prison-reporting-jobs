package uk.gov.justice.digital.common;

import java.util.Map;
import java.util.Objects;

public class SourceMapping {

    private final String sourceTable;
    private final String destinationTable;
    private final Map<String, String> tableAliases;
    private final Map<ColumnMapping, ColumnMapping> columnMap;

    private SourceMapping(
            String sourceTable,
            String destinationTable,
            Map<String, String> tableAliases,
            Map<ColumnMapping, ColumnMapping> columnMap
    ) {
        this.sourceTable = sourceTable;
        this.destinationTable = destinationTable;
        this.tableAliases = tableAliases;
        this.columnMap = columnMap;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public String getDestinationTable() {
        return destinationTable;
    }

    public Map<String, String> getTableAliases() {
        return tableAliases;
    }

    public Map<ColumnMapping, ColumnMapping> getColumnMap() {
        return columnMap;
    }

    public static SourceMapping create(
            String sourceTable,
            String destinationTable,
            Map<String, String> tableAliases,
            Map<ColumnMapping, ColumnMapping> columnMap
    ) {
        return new SourceMapping(sourceTable, destinationTable, tableAliases, columnMap);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SourceMapping that = (SourceMapping) o;
        return Objects.equals(sourceTable, that.sourceTable) &&
                Objects.equals(destinationTable, that.destinationTable) &&
                Objects.equals(tableAliases, that.tableAliases) &&
                Objects.equals(columnMap, that.columnMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceTable, destinationTable, tableAliases, columnMap);
    }

    @Override
    public String toString() {
        return "SourceMapping{" +
                "sourceTable='" + sourceTable + '\'' +
                ", destinationTable='" + destinationTable + '\'' +
                ", tableAliases=" + tableAliases +
                ", columnMap=" + columnMap +
                '}';
    }
}
