package uk.gov.justice.digital.exception;

public class NoSchemaNoDataException extends Exception {

    private static final long serialVersionUID = -238644603056367129L;


    public NoSchemaNoDataException(String message, Throwable cause) {
        super(message, cause);
    }
}
