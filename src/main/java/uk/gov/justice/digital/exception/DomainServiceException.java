package uk.gov.justice.digital.exception;

public class DomainServiceException extends Exception {
    private static final long serialVersionUID = 8743700622826094603L;

    public DomainServiceException(String errorMessage) {
        super(errorMessage);
    }

    public DomainServiceException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

}