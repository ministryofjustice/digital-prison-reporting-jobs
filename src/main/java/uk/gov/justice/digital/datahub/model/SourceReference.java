package uk.gov.justice.digital.datahub.model;

import lombok.Data;
import org.apache.spark.sql.types.StructType;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.lang.String.format;

@Data
public class SourceReference {
    private final String key;
    private final String namespace;
    private final String source;
    private final String table;
    private final PrimaryKey primaryKey;
    private final String versionId;
    private final StructType schema;
    private final SensitiveColumns sensitiveColumns;

    public String getFullyQualifiedTableName() {
        return format("%s.%s", source, table);
    }

    public static class PrimaryKey {

        private final Collection<String> keys;

        public PrimaryKey(Collection<?> o) {
            keys = o.stream().map(x -> Objects.toString(x, null)).collect(Collectors.toList());
        }

        public PrimaryKey(String s) {
            keys = Collections.singletonList(s);
        }

        public String getSparkCondition(final String source, final String target) {
            return keys.stream()
                    .map(s -> source + "." + s + " = " + target + "." + s)
                    .collect(Collectors.joining(" and "));
        }

        public Collection<String> getKeyColumnNames() {
            return Collections.unmodifiableCollection(keys);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PrimaryKey)) return false;
            PrimaryKey that = (PrimaryKey) o;
            return Objects.equals(keys, that.keys);
        }

        @Override
        public int hashCode() {
            return Objects.hash(keys);
        }
    }

    public static class SensitiveColumns {

        private final Collection<String> columns;

        public SensitiveColumns(Collection<?> o) {
            columns = o.stream().map(x -> Objects.toString(x, null)).collect(Collectors.toList());
        }

        public SensitiveColumns(String s) {
            columns = Collections.singletonList(s);
        }

        public Collection<String> getSensitiveColumnNames() {
            return Collections.unmodifiableCollection(columns);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SensitiveColumns)) return false;
            SensitiveColumns that = (SensitiveColumns) o;
            return Objects.equals(columns, that.columns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(columns);
        }
    }
}
