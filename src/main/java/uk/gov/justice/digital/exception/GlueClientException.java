package uk.gov.justice.digital.exception;

public class GlueClientException extends RuntimeException {

    private static final long serialVersionUID = -7832951603298104749L;

    public GlueClientException(String errorMessage) {
        super(errorMessage);
    }

    public GlueClientException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

}