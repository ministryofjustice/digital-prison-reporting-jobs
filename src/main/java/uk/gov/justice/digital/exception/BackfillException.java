package uk.gov.justice.digital.exception;

public class BackfillException extends RuntimeException {
    private static final long serialVersionUID = -2244810603090098293L;

    public BackfillException(String message) {
        super(message);
    }
}
