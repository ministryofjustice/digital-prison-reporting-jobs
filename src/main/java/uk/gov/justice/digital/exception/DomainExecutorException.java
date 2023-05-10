package uk.gov.justice.digital.exception;

public class DomainExecutorException extends Exception {

    private static final long serialVersionUID = 2144285372585522225L;

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
