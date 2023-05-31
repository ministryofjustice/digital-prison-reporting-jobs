package uk.gov.justice.digital.service;

import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.glue.GlueSchemaClient;
import uk.gov.justice.digital.converter.avro.AvroToSparkSchemaConverter;
import uk.gov.justice.digital.domain.model.SourceReference;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.test.ResourceLoader.getResource;

@ExtendWith(MockitoExtension.class)
public class SourceReferenceServiceTest {

    private static final String RESOURCE_PATH = "/contracts";
    private static final String AGENCY_INTERNAL_LOCATIONS_CONTRACT = "agency-internal-locations-contract.avsc";

    private static final AvroToSparkSchemaConverter converter = new AvroToSparkSchemaConverter();

    @Mock
    private GlueSchemaClient client;


    private SourceReferenceService underTest;

    @BeforeEach
    public void setup() {
        underTest = new SourceReferenceService(client, converter);
    }


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

    @Test
    public void getSourceReferenceShouldReturnReferenceFromClientWhereItExists() {
        when(client.getSchema("oms_owner.agency_internal_locations"))
                .thenReturn(Optional.of(getResource(RESOURCE_PATH + "/" + AGENCY_INTERNAL_LOCATIONS_CONTRACT)));

        val result = underTest.getSourceReference("oms_owner", "agency_internal_locations");
        // TODO - check response in detail
        assertTrue(result.isPresent());
    }

}
