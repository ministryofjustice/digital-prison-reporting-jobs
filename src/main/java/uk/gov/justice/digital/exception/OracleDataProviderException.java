package uk.gov.justice.digital.exception;

public class OracleDataProviderException extends RuntimeException {
    private static final long serialVersionUID = -5249877118604183233L;

    public OracleDataProviderException(String errorMessage) {
        super(errorMessage);
    }

    public OracleDataProviderException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

}
