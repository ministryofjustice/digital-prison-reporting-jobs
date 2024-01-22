package uk.gov.justice.digital.exception;

/**
 * Thrown when the data provider cannot read the input data due to incompatible schemas.
 */
public class DataProviderFailedMergingSchemasException extends RuntimeException {

    private static final long serialVersionUID = -8685733006029513888L;
    public DataProviderFailedMergingSchemasException(String message, Throwable cause) {
        super(message, cause);
    }
}
