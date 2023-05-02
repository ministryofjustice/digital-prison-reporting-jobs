package uk.gov.justice.digital.exception;

public class DomainExecutorException extends Throwable {

    private static final long serialVersionUID = 1L;
    public DomainExecutorException(String errorMessage) {
        super();
    }

    public DomainExecutorException(Throwable t) { super(t); }
}
