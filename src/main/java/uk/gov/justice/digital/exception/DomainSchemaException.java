package uk.gov.justice.digital.exception;

public class DomainSchemaException extends Exception {

    public DomainSchemaException(String errorMessage) {
        super(errorMessage);
    }

    public DomainSchemaException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }
}