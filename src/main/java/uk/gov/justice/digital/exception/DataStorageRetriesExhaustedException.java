package uk.gov.justice.digital.exception;

/**
 * Thrown when the DataStorageService exceeds the configured number of retries/attempts.
 */
public class DataStorageRetriesExhaustedException extends DataStorageException {
    public DataStorageRetriesExhaustedException(Throwable cause) {
        super(cause);
    }
}
