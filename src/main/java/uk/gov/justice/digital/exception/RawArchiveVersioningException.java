package uk.gov.justice.digital.exception;

public class RawArchiveVersioningException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public RawArchiveVersioningException(String message) {
        super(message);
    }
}
