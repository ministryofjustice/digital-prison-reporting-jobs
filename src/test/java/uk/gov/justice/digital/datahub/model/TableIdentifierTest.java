package uk.gov.justice.digital.datahub.model;

import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TableIdentifierTest {

    @Test
    void shouldGeneratePathFromInstanceFields() {
        val underTest = new TableIdentifier("s3://foo", "bar", "baz", "blah");
        assertEquals("s3://foo/baz/blah", underTest.toPath());
    }

}
