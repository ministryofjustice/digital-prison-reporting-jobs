package uk.gov.justice.digital.exception;

public class JDBCGlueConnectionDetailsException extends RuntimeException {

    private static final long serialVersionUID = -9016956480838963212L;

    public JDBCGlueConnectionDetailsException(String errorMessage) {
        super(errorMessage);
    }
}
