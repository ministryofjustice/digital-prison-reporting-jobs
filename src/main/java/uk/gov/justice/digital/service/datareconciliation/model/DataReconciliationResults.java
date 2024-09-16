package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.Getter;

@Getter
public class DataReconciliationResults {

    private final CurrentStateTotalCounts currentStateTotalCounts;
    private final ChangeDataTotalCounts changeDataTotalCounts;

    public DataReconciliationResults(
            CurrentStateTotalCounts currentStateTotalCounts,
            ChangeDataTotalCounts changeDataTotalCounts
    ) {
        this.currentStateTotalCounts = currentStateTotalCounts;
        this.changeDataTotalCounts = changeDataTotalCounts;
    }

    public boolean isSuccess() {
        // TODO: test
        return currentStateTotalCounts.countsMatch() && changeDataTotalCounts.countsMatch();
    }

    public String summary() {
        // TODO: test
        return currentStateTotalCounts.summary() + "\n\n" + changeDataTotalCounts.summary();
    }
}
