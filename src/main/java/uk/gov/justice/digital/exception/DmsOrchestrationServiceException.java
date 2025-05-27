package uk.gov.justice.digital.exception;

public class DmsOrchestrationServiceException extends RuntimeException {

    private static final long serialVersionUID = -8293031160572331420L;

    public DmsOrchestrationServiceException(String errorMessage) {
        super(errorMessage);
    }
}
