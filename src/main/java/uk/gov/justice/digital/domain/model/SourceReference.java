package uk.gov.justice.digital.domain.model;

import lombok.Data;
import org.apache.spark.sql.types.StructType;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;

@Data
public class SourceReference {
    private final String key;
    private final String source;
    private final String table;
    private final PrimaryKey primaryKey;
    private final StructType schema;

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
                    // TODO: I don't think toLowerCase is necessary
                    .map(String::toLowerCase)
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
}
