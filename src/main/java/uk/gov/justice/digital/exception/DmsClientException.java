package uk.gov.justice.digital.exception;

public class DmsClientException extends RuntimeException {

    private static final long serialVersionUID = -6423158118665870705L;

    public DmsClientException(String errorMessage) {
        super(errorMessage);
    }

    public DmsClientException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

}