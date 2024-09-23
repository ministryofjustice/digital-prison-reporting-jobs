package uk.gov.justice.digital.exception;

public class ReconciliationDataSourceException extends RuntimeException {
    private static final long serialVersionUID = 1625487016149051873L;

    public ReconciliationDataSourceException(String errorMessage) {
        super(errorMessage);
    }

    public ReconciliationDataSourceException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

}
