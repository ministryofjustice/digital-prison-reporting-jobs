package uk.gov.justice.digital.service.datareconciliation.model;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.val;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.amazonaws.services.cloudwatch.model.StandardUnit.Count;

/**
 * Represents the results of running the total counts data reconciliation for the "current state" data in DataHub
 */
@EqualsAndHashCode
@ToString
public class CurrentStateTotalCounts implements DataReconciliationResult {

    private static final String COUNT_METRIC_NAME = "CurrentStateCount";

    private final Map<String, CurrentStateTableCount> tableToResult = new HashMap<>();

    public void put(String fullTableName, CurrentStateTableCount currentStateTableCount) {
        tableToResult.put(fullTableName, currentStateTableCount);
    }

    /**
     * Returns the value to which the specified key is mapped, or
     *  {@code null} if this map contains no mapping for the key
     */
    public CurrentStateTableCount get(String fullTableName) {
        return tableToResult.get(fullTableName);
    }

    @Override
    public boolean isSuccess() {
        return tableToResult.values().stream().allMatch(CurrentStateTableCount::countsMatch);
    }

    @Override
    public String summary() {
        StringBuilder sb = new StringBuilder("Current State Total Counts ");
        if (isSuccess()) {
            sb.append("MATCH:\n");
        } else {
            sb.append("DO NOT MATCH:\n");
        }

        for (val entrySet: tableToResult.entrySet()) {
            val tableName = entrySet.getKey();
            val currentStateCountTableResult = entrySet.getValue();
            sb.append("For table ").append(tableName).append(":\n");
            sb.append("\t").append(currentStateCountTableResult.summary()).append("\n");
        }

        return sb.toString();
    }

    @Override
    public Set<MetricDatum> toCloudwatchMetricData() {
        Set<MetricDatum> metrics = new HashSet<>();

        for (Map.Entry<String, CurrentStateTableCount> entry : tableToResult.entrySet()) {
            String table = entry.getKey();
            CurrentStateTableCount counts = entry.getValue();

            metrics.add(currentStateCountMetricDatum("source", table, counts.getDataSourceCount()));
            metrics.add(currentStateCountMetricDatum("structured", table, counts.getStructuredCount()));
            metrics.add(currentStateCountMetricDatum("curated", table, counts.getCuratedCount()));
            Long operationalDataStoreCount = counts.getOperationalDataStoreCount();
            if (operationalDataStoreCount != null) {
                currentStateCountMetricDatum("ods", table, operationalDataStoreCount);
            }
        }
        return metrics;
    }

    private MetricDatum currentStateCountMetricDatum(String dataStore, String tableName, long count) {
        return new MetricDatum()
                .withMetricName(COUNT_METRIC_NAME)
                .withUnit(Count)
                .withDimensions(
                        // Data store, e.g. source or curated
                        new Dimension().withName("datastore").withValue(dataStore),
                        // The table name
                        new Dimension().withName("table").withValue(tableName)
                )
                .withValue((double) count);
    }
}
