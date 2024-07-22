package uk.gov.justice.digital.exception;

public class SchemaNotFoundException extends RuntimeException {
    private static final long serialVersionUID = -1077953479335895928L;

    public SchemaNotFoundException(String message) {
        super(message);
    }
}
