package uk.gov.justice.digital.service.datareconciliation.model;

import com.amazonaws.services.cloudwatch.model.MetricDatum;
import lombok.Getter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.amazonaws.services.cloudwatch.model.StandardUnit.Count;

@Getter
public class DataReconciliationResults implements DataReconciliationResult {

    private static final String TABLES_FAILED_RECONCILIATION_METRIC_NAME = "TablesFailedReconciliation";

    private final List<DataReconciliationResult> results;

    public DataReconciliationResults(List<DataReconciliationResult> results) {
        this.results = results;
    }

    @Override
    public boolean isSuccess() {
        return results.stream().allMatch(DataReconciliationResult::isSuccess);
    }

    @Override
    public String summary() {
        return results.stream()
                .map(DataReconciliationResult::summary)
                .reduce("", (s1, s2) -> s1 + "\n\n" + s2);
    }

    public Set<MetricDatum> toCloudwatchMetricData() {
        Set<MetricDatum> metrics = results
                .stream()
                .map(DataReconciliationResult::toCloudwatchMetricData)
                .reduce(
                        new HashSet<>(),
                        (s1, s2) -> {
                            s1.addAll(s2);
                            return s1;
                        });

        long numTablesFailingReconciliation = results.stream().filter(r -> !r.isSuccess()).count();

        metrics.add(
                new MetricDatum()
                        .withMetricName(TABLES_FAILED_RECONCILIATION_METRIC_NAME)
                        .withUnit(Count)
                        .withValue((double) numTablesFailingReconciliation)
        );
        return metrics;
    }

}
