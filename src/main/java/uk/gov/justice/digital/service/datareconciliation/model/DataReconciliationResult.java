package uk.gov.justice.digital.service.datareconciliation.model;

import com.amazonaws.services.cloudwatch.model.MetricDatum;

import java.util.Set;

/**
 * The result of a data reconciliation check, or an aggregate result of multiple checks
 */
public interface DataReconciliationResult {
    boolean isSuccess();
    String summary();
    Set<MetricDatum> toCloudwatchMetricData();
}
