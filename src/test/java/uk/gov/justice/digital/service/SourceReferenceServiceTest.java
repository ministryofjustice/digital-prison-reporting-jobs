package uk.gov.justice.digital.service;

import lombok.val;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.domain.model.SourceReference;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SourceReferenceServiceTest {

    private static final SourceReferenceService underTest = new SourceReferenceService();

    @Test
    public void getSourceReferenceShouldReturnCorrectReferenceForExistingSourceAndTable() {
        val sourceReference = underTest.getSourceReference("OMS_OWNER", "OFFENDERS");

        assertEquals(Optional.of("nomis"), sourceReference.map(SourceReference::getSource));
        assertEquals(Optional.of("offenders"), sourceReference.map(SourceReference::getTable));
    }

    @Test
    public void getSourceReferenceShouldReturnCorrectReferenceIrrespectiveOfCapitalizationOfParameters() {
        val sourceReference = underTest.getSourceReference("oMs_oWnEr", "oFfEnDeRs");

        assertEquals(Optional.of("nomis"), sourceReference.map(SourceReference::getSource));
        assertEquals(Optional.of("offenders"), sourceReference.map(SourceReference::getTable));
    }

    @Test
    public void getSourceReferenceShouldReturnAnEmptyOptionalIfNoReferenceIsFound() {
        assertEquals(Optional.empty(), underTest.getSourceReference("DOES_NOT", "EXIST"));
    }

}
