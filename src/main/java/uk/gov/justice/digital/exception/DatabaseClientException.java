package uk.gov.justice.digital.exception;

public class DatabaseClientException extends Exception {

    private static final long serialVersionUID = -898688908541969061L;

    public DatabaseClientException(String errorMessage) {
        super(errorMessage);
    }

    public DatabaseClientException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

}