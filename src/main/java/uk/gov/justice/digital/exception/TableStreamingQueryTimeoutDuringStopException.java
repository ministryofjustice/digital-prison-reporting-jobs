package uk.gov.justice.digital.exception;

public class TableStreamingQueryTimeoutDuringStopException extends RuntimeException {

    private static final long serialVersionUID = 1025138831490044901L;

    public TableStreamingQueryTimeoutDuringStopException(Throwable cause) {
        super(cause);
    }
}