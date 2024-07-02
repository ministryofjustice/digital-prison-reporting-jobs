package uk.gov.justice.digital.exception;

public class OperationalDataStoreException extends RuntimeException {

    private static final long serialVersionUID = 8570364121994003864L;

    public OperationalDataStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
