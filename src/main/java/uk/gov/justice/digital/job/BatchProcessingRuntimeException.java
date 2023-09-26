package uk.gov.justice.digital.job;

public class BatchProcessingRuntimeException extends RuntimeException {

    public BatchProcessingRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
