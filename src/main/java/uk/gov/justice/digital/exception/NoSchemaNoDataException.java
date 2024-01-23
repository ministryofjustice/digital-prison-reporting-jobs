package uk.gov.justice.digital.exception;

/**
 * Thrown when there is no data to read and no schema to use to watch for the data to arrive.
 */
public class NoSchemaNoDataException extends RuntimeException {

    private static final long serialVersionUID = -238644603056367129L;


    public NoSchemaNoDataException(String message, Throwable cause) {
        super(message, cause);
    }
}
