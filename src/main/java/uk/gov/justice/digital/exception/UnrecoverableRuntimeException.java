package uk.gov.justice.digital.exception;

public class UnrecoverableRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 378644558792938256L;

    public UnrecoverableRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
