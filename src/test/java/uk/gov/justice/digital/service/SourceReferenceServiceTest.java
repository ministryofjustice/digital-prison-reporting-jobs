package uk.gov.justice.digital.service;

import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.glue.GlueSchemaClient;
import uk.gov.justice.digital.client.glue.GlueSchemaClient.GlueSchemaResponse;
import uk.gov.justice.digital.converter.avro.AvroToSparkSchemaConverter;
import uk.gov.justice.digital.domain.model.SourceReference;

import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.test.ResourceLoader.getResource;

@ExtendWith(MockitoExtension.class)
public class SourceReferenceServiceTest {

    private static final String RESOURCE_PATH = "/contracts";
    private static final String AGENCY_INTERNAL_LOCATIONS_CONTRACT = "agency-internal-locations.avsc";
    private static final String OFFENDERS_CONTRACT = "offenders.avsc";

    private static final AvroToSparkSchemaConverter converter = new AvroToSparkSchemaConverter();

    @Mock
    private GlueSchemaClient client;


    private SourceReferenceService underTest;

    @BeforeEach
    public void setup() {
        underTest = new SourceReferenceService(client, converter);
    }


    @Test
    public void shouldReturnCorrectReferenceForExistingSourceAndTable() {
        val sourceReference = underTest.getSourceReference("OMS_OWNER", "OFFENDERS");

        assertEquals(Optional.of("nomis"), sourceReference.map(SourceReference::getSource));
        assertEquals(Optional.of("offenders"), sourceReference.map(SourceReference::getTable));
    }

    @Test
    public void shouldReturnCorrectReferenceIrrespectiveOfCapitalizationOfParameters() {
        val sourceReference = underTest.getSourceReference("oMs_oWnEr", "oFfEnDeRs");

        assertEquals(Optional.of("nomis"), sourceReference.map(SourceReference::getSource));
        assertEquals(Optional.of("offenders"), sourceReference.map(SourceReference::getTable));
    }

    @Test
    public void shouldReturnAnEmptyOptionalIfNoReferenceIsFound() {
        assertEquals(Optional.empty(), underTest.getSourceReference("DOES_NOT", "EXIST"));
    }

    @Test
    public void shouldReturnReferenceFromClientWhereItExists() {
        val schemaId = UUID.randomUUID().toString();
        val schemaResponse = new GlueSchemaResponse(
                schemaId,
                getResource(RESOURCE_PATH + "/" + OFFENDERS_CONTRACT)
        );
        when(client.getSchema("oms_owner.offenders")).thenReturn(Optional.of(schemaResponse));

        val result = underTest.getSourceReference("oms_owner", "offenders");

        assertTrue(result.isPresent());

        val sourceReference = result.get();

        assertEquals(schemaId, sourceReference.getKey());
        assertEquals("nomis", sourceReference.getSource());
        assertEquals("offenders", sourceReference.getTable());
        assertEquals("OFFENDER_ID", sourceReference.getPrimaryKey());
        // See AvroToSparkSchemaConverter for more detailed testing of the conversion.
        assertNotNull(sourceReference.getSchema());
    }

    // We are going to add an optional _XXX version suffix to the name field as part of the publishing process.
    // This is so that any update triggers a new version publication. (By default only avro changes trigger a new
    // version so without this changes to our custom fields would not trigger an update).
    @Test
    public void shouldStripVersionSuffixFromNameAttribute() {
        val schemaId = UUID.randomUUID().toString();
        val tableName = "AGENCY_INTERNAL_LOCATIONS";
        val schemaResponse = new GlueSchemaResponse(
                schemaId,
                getResource(RESOURCE_PATH + "/" + AGENCY_INTERNAL_LOCATIONS_CONTRACT)
                        .replace(tableName, tableName + "_17")
        );
        when(client.getSchema("oms_owner.agency_internal_locations")).thenReturn(Optional.of(schemaResponse));

        val result = underTest.getSourceReference("oms_owner", "agency_internal_locations");

        assertTrue(result.isPresent());

        val sourceReference = result.get();

        assertEquals(tableName.toLowerCase(Locale.ENGLISH), sourceReference.getTable(), "Version suffix should be removed from table name");
    }

    @Test
    public void shouldThrowExceptionIfSchemaCannotBeParsed() {
        when(client.getSchema("some.schema"))
                .thenReturn(Optional.of(new GlueSchemaResponse(UUID.randomUUID().toString(), "This is not valid JSON")));

        assertThrows(RuntimeException.class, () -> underTest.getSourceReference("some", "schema"));
    }

}