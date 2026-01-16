package uk.gov.justice.digital.exception;

public class ConfigReaderClientException extends RuntimeException {

    private static final long serialVersionUID = -6478552657569286384L;

    public ConfigReaderClientException(String errorMessage) {
        super(errorMessage);
    }

    public ConfigReaderClientException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

}
