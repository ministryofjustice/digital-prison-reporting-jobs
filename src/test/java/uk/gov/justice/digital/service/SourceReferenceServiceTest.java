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

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;
import static org.hamcrest.Matchers.*;
import static uk.gov.justice.digital.test.ResourceLoader.getResource;
import static uk.gov.justice.digital.test.SparkTestHelpers.containsTheSameElementsInOrderAs;

@ExtendWith(MockitoExtension.class)
public class SourceReferenceServiceTest {

    private static final String RESOURCE_PATH = "/contracts";
    private static final String AGENCY_INTERNAL_LOCATIONS_CONTRACT = "agency-internal-locations-dummy.avsc";
    private static final String OFFENDERS_CONTRACT = "offenders-dummy.avsc";
    private static final String COMPOSITE_KEY_CONTRACT = "composite-key.avsc";

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
    public void getSourceReferenceOrThrowShouldThrowWhereItDoesNotExist() {
        when(client.getSchema("source.schema")).thenReturn(Optional.empty());
        assertThrows(RuntimeException.class, () -> {
            underTest.getSourceReferenceOrThrow("oms_owner", "offenders");
        });
    }

    @Test
    public void getSourceReferenceOrThrowShouldReturnReferenceWhereItExists() {
        val schemaId = UUID.randomUUID().toString();
        val schemaResponse = new GlueSchemaResponse(
                schemaId,
                getResource(RESOURCE_PATH + "/" + OFFENDERS_CONTRACT)
        );
        when(client.getSchema("oms_owner.offenders")).thenReturn(Optional.of(schemaResponse));

        val result = underTest.getSourceReferenceOrThrow("oms_owner", "offenders");
        assertEquals(schemaId, result.getKey());
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

        val pk = new SourceReference.PrimaryKey("OFFENDER_ID");

        assertEquals(schemaId, sourceReference.getKey());
        assertEquals("nomis", sourceReference.getSource());
        assertEquals("offenders", sourceReference.getTable());
        assertEquals(pk, sourceReference.getPrimaryKey());
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

    @Test
    public void shouldGetASourceReferenceWithACompositeSchema() {
        val schemaId = UUID.randomUUID().toString();
        val tableName = "COMPOSITE";
        val schemaResponse = new GlueSchemaResponse(
                schemaId,
                getResource(RESOURCE_PATH + "/" + COMPOSITE_KEY_CONTRACT)
        );

        when(client.getSchema("oms_owner.composite")).thenReturn(Optional.of(schemaResponse));

        val result = underTest.getSourceReference("oms_owner", "composite");


    }

    @Test
    public void shouldConcatenatePrimaryKeysIntoASqlCondition() {
        SourceReference.PrimaryKey pk = new SourceReference.PrimaryKey(Arrays.asList("key1"));
        assertEquals("source.key1 = target.key1", pk.getSparkCondition("source", "target"));

        pk = new SourceReference.PrimaryKey(Arrays.asList("key1", "key2"));
        assertEquals("source.key1 = target.key1 and source.key2 = target.key2", pk.getSparkCondition("source", "target"));

    }

    @Test
    public void shouldReturnAListOfAllSourceReferences() {
        List<GlueSchemaResponse> expectedSchemas = new ArrayList<>();

        IntStream.range(0, 3).forEach(index -> {
                    String schemaId = String.valueOf(index);
                    String avroString = getResource(RESOURCE_PATH + "/" + OFFENDERS_CONTRACT);
                    val schemaResponse = new GlueSchemaResponse(schemaId, avroString);
                    expectedSchemas.add(schemaResponse);
                }
        );

        when(client.getAllSchemas()).thenReturn(expectedSchemas);

        List<SourceReference> result = underTest.getAllSourceReferences();

        List<String> actualIds = result
                .stream()
                .map(SourceReference::getKey)
                .collect(Collectors.toList());

        List<String> expectedIds = expectedSchemas.stream().map(GlueSchemaResponse::getId).collect(Collectors.toList());

        assertThat(actualIds, containsTheSameElementsInOrderAs(expectedIds));
    }

    @Test
    public void shouldReturnAnEmptyListWhenNoSchemaIsFound() {
        when(client.getAllSchemas()).thenReturn(Collections.emptyList());

        assertThat((Collection<SourceReference>) underTest.getAllSourceReferences(), is(empty()));
    }
}