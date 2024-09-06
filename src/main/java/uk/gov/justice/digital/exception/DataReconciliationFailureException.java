package uk.gov.justice.digital.exception;

import lombok.Getter;
import uk.gov.justice.digital.service.datareconciliation.CurrentStateCountResults;

@Getter
public class DataReconciliationFailureException extends RuntimeException {

    private static final long serialVersionUID = -7214599810476201398L;

    private final CurrentStateCountResults results;

    public DataReconciliationFailureException(CurrentStateCountResults results) {
        super("Data reconciliation failed");
        this.results = results;
    }

}
