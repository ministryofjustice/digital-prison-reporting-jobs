package uk.gov.justice.digital.exception;

public class NomisDataAccessException extends RuntimeException {
    private static final long serialVersionUID = -5249877118604183233L;

    public NomisDataAccessException(String errorMessage) {
        super(errorMessage);
    }

    public NomisDataAccessException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

}
