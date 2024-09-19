package uk.gov.justice.digital.service.datareconciliation.model;

/**
 * The result of a data reconciliation check, or an aggregate result of multiple checks
 */
public interface DataReconciliationResult {
    boolean isSuccess();
    String summary();
}
