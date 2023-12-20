package uk.gov.justice.digital.exception;

public class ConfigServiceException extends RuntimeException {

    private static final long serialVersionUID = -7779159905858656378L;

    public ConfigServiceException(String errorMessage) {
        super(errorMessage);
    }

    @SuppressWarnings("unused")
    public ConfigServiceException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

}