package uk.gov.justice.digital.exception;

public class DataStorageException extends RuntimeException {

    private static final long serialVersionUID = 4818463902468617768L;

    public DataStorageException(String errorMessage) {
        super(errorMessage);
    }

    public DataStorageException(Throwable cause) {
        super(cause);
    }

    public DataStorageException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

}