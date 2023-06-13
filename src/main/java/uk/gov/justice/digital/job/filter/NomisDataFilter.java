package uk.gov.justice.digital.job.filter;

import lombok.val;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

public class NomisDataFilter {

    private final Function<Entry<String, Object>, Entry<String, Object>> filter;

    public NomisDataFilter(StructType schema) {
        filter = combineFieldFilters(Arrays.asList(
                new TimestampToDateFieldFilter(schema),
                new SparkTimestampFieldFilter(schema)
        ));
    }

    public Map<String, Object> apply(Map<String, Object> rawData) {
        val filteredMap = new HashMap<String, Object>();

        rawData.entrySet()
                .forEach(entry -> {
                   val updatedEntry = filter.apply(entry);
                   filteredMap.put(updatedEntry.getKey(), updatedEntry.getValue());
                });

        return filteredMap;
    }

    private Function<Entry<String, Object>, Entry<String, Object>> combineFieldFilters(List<FieldFilter> filters) {
        return filters.stream()
                .map(filter -> (Function<Entry<String, Object>, Entry<String, Object>>) filter::applyIfEligible)
                .reduce(Function.identity(), Function::andThen);
    }
}
