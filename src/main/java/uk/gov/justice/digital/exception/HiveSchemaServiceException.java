package uk.gov.justice.digital.exception;

public class HiveSchemaServiceException extends RuntimeException {
    private static final long serialVersionUID = 4896512127757385022L;

    public HiveSchemaServiceException(String errorMessage) {
        super(errorMessage);
    }

    public HiveSchemaServiceException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

}