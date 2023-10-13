package uk.gov.justice.digital.exception;

/**
 * Thrown when the DataStorageService exceeds the configured number of retries/attempts.
 */
public class DataStorageRetriesExhaustedException extends DataStorageException {

    private static final long serialVersionUID = -2122315875377665022L;
    public DataStorageRetriesExhaustedException(Throwable cause) {
        super(cause);
    }
}
