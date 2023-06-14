package uk.gov.justice.digital.job.filter;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class FieldFilter {

    protected final Set<String> applicableFields;

    public FieldFilter(Set<String> applicableFields) {
        this.applicableFields = applicableFields;
    }

    public final Entry<String, Object> apply(Entry<String, Object> entry) {
        return (isApplicable(entry.getKey()))
                ? applyFilterToEntry(entry)
                : entry;
    }

    protected abstract Entry<String, Object> applyFilterToEntry(Entry<String, Object> entry);

    private boolean isApplicable(String fieldName) {
        return applicableFields.contains(fieldName);
    }

    protected static Set<String> getApplicableFieldsFromSchema(StructType schema,
                                                               Function<StructField, Boolean> predicate) {
        return Arrays.stream(schema.fields())
                .filter(predicate::apply)
                .map(StructField::name)
                .collect(Collectors.toSet());
    }

}
