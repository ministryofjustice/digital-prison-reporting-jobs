package uk.gov.justice.digital.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
class SourceReferenceTest {
    private final static String OMS_OWNER_INTERNAL_NAME = "nomis";
    private final static String NOT_FOUND = "not_found";
    @Test
    public void shouldReturnValidInternalSource() {
        assertEquals(OMS_OWNER_INTERNAL_NAME, SourceReference.getInternalSource("oms_owner"));
    }

    @Test
    public void shouldReturnValidInternalSourceWhenUpperCaseSourceFound() {
        assertEquals(OMS_OWNER_INTERNAL_NAME, SourceReference.getInternalSource("OMS_OWNER"));
    }

    @Test
    public void shouldReturnSameSourceWhenSourceNotFound() {
        assertEquals(NOT_FOUND, SourceReference.getInternalSource("not_found"));
    }
}