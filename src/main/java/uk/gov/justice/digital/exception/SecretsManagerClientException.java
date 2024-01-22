package uk.gov.justice.digital.exception;

public class SecretsManagerClientException extends RuntimeException {

    private static final long serialVersionUID = 8108611015208162383L;

    public SecretsManagerClientException(String errorMessage) {
        super(errorMessage);
    }

    public SecretsManagerClientException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

}