package uk.gov.justice.digital.exception;

public class DomainSchemaException extends Exception {

    private static final long serialVersionUID = -4791350148332245527L;

    public DomainSchemaException(String errorMessage) {
        super(errorMessage);
    }

    public DomainSchemaException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }
}