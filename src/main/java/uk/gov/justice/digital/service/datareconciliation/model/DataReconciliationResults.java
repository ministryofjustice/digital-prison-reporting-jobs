package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.Getter;

import java.util.List;

@Getter
public class DataReconciliationResults implements DataReconciliationResult {

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

    public long numReconciliationChecksFailing() {
        return results.stream().filter(r -> !r.isSuccess()).count();
    }
}
