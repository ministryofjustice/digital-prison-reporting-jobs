package uk.gov.justice.digital.exception;

/**
 * Indicates that a maintenance operation has failed
 */
public class MaintenanceOperationFailedException extends RuntimeException {
    private static final long serialVersionUID = 2268576462537095013L;

    public MaintenanceOperationFailedException(String errorMessage) {
        super(errorMessage);
    }

    public MaintenanceOperationFailedException(Throwable t) {
        super(t);
    }

    public MaintenanceOperationFailedException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

}
