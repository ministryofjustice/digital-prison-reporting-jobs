package uk.gov.justice.digital.common;

import java.util.Objects;

public class ColumnMapping {

    private final String tableName;
    private final String columnName;

    private ColumnMapping(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public static ColumnMapping create(String tableName, String columnName) {
        return new ColumnMapping(tableName, columnName);
    }

    public ColumnMapping upperCaseColumnName() {
        return new ColumnMapping(tableName, columnName.toUpperCase());
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof ColumnMapping))
            return false;
        ColumnMapping other = (ColumnMapping) o;
        return Objects.equals(this.tableName, other.tableName) && Objects.equals(this.columnName, other.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, columnName);
    }

    @Override
    public String toString() {
        return "ColumnMapping{" +
                "tableName='" + tableName + '\'' +
                ", columnName='" + columnName + '\'' +
                '}';
    }
}
