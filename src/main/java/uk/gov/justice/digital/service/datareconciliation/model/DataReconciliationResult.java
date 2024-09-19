package uk.gov.justice.digital.service.datareconciliation.model;

public interface DataReconciliationResult {
    boolean isSuccess();
    String summary();
}
