package uk.gov.justice.digital.exception;

public class DomainExecutorException extends Exception {

    public DomainExecutorException(String errorMessage) {
        super(errorMessage);
    }

    public DomainExecutorException(Throwable t) {
        super(t);
    }

    public DomainExecutorException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

}
