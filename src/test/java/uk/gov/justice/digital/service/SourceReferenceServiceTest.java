package uk.gov.justice.digital.service;

import com.google.common.collect.ImmutableSet;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3SchemaClient;
import uk.gov.justice.digital.converter.avro.AvroToSparkSchemaConverter;
import uk.gov.justice.digital.datahub.model.SourceReference;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.client.s3.S3SchemaClient.S3SchemaResponse;
import static uk.gov.justice.digital.test.ResourceLoader.getResource;
import static uk.gov.justice.digital.test.SparkTestHelpers.containsTheSameElementsInOrderAs;

@ExtendWith(MockitoExtension.class)
public class SourceReferenceServiceTest {

    private static final String RESOURCE_PATH = "/contracts";
    private static final String AGENCY_INTERNAL_LOCATIONS_CONTRACT = "agency-internal-locations-dummy.avsc";
    private static final String OFFENDERS_CONTRACT = "offenders-dummy.avsc";
    private static final String COMPOSITE_KEY_CONTRACT = "composite-key.avsc";
    private static final String VERSION_ID = UUID.randomUUID().toString();

    private static final AvroToSparkSchemaConverter converter = new AvroToSparkSchemaConverter();

    @Mock
    private S3SchemaClient client;


    private SourceReferenceService underTest;

    @BeforeEach
    public void setup() {
        underTest = new SourceReferenceService(client, converter);
    }

    @Test
    public void shouldReturnCorrectReferenceIrrespectiveOfCapitalizationOfParameters() {
        val schemaId = UUID.randomUUID().toString();
        val schemaResponse = new S3SchemaResponse(
                schemaId,
                getResource(RESOURCE_PATH + "/" + OFFENDERS_CONTRACT),
                VERSION_ID
        );
        when(client.getSchema("oms_owner/offenders")).thenReturn(Optional.of(schemaResponse));

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
        val schemaResponse = new S3SchemaResponse(
                schemaId,
                getResource(RESOURCE_PATH + "/" + OFFENDERS_CONTRACT),
                VERSION_ID
        );
        when(client.getSchema("oms_owner/offenders")).thenReturn(Optional.of(schemaResponse));

        val result = underTest.getSourceReference("oms_owner", "offenders");

        assertTrue(result.isPresent());

        val sourceReference = result.get();

        val pk = new SourceReference.PrimaryKey("OFFENDER_ID");

        assertEquals(schemaId, sourceReference.getKey());
        assertEquals("nomis", sourceReference.getSource());
        assertEquals("offenders", sourceReference.getTable());
        assertEquals(pk, sourceReference.getPrimaryKey());
        assertEquals(VERSION_ID, sourceReference.getVersionId());
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
        val schemaResponse = new S3SchemaResponse(
                schemaId,
                getResource(RESOURCE_PATH + "/" + AGENCY_INTERNAL_LOCATIONS_CONTRACT)
                        .replace(tableName, tableName + "_17"),
                VERSION_ID
        );
        when(client.getSchema("oms_owner/agency_internal_locations")).thenReturn(Optional.of(schemaResponse));

        val result = underTest.getSourceReference("oms_owner", "agency_internal_locations");

        assertTrue(result.isPresent());

        val sourceReference = result.get();

        assertEquals(tableName.toLowerCase(Locale.ENGLISH), sourceReference.getTable(), "Version suffix should be removed from table name");
    }

    @Test
    public void shouldThrowExceptionIfSchemaCannotBeParsed() {
        when(client.getSchema("some/schema"))
                .thenReturn(Optional.of(new S3SchemaResponse(UUID.randomUUID().toString(), "This is not valid JSON", VERSION_ID)));

        assertThrows(RuntimeException.class, () -> underTest.getSourceReference("some", "schema"));
    }

    @Test
    public void shouldGetASourceReferenceWithACompositeSchema() {
        val schemaId = UUID.randomUUID().toString();
        val schemaResponse = new S3SchemaResponse(
                schemaId,
                getResource(RESOURCE_PATH + "/" + COMPOSITE_KEY_CONTRACT),
                VERSION_ID
        );

        when(client.getSchema("oms_owner/composite")).thenReturn(Optional.of(schemaResponse));

        assertTrue(underTest.getSourceReference("oms_owner", "composite").isPresent());
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
        ImmutableSet<ImmutablePair<String, String>> schemaGroup = ImmutableSet
                .of(ImmutablePair.of("test_schema", "test_table"));
        List<S3SchemaResponse> expectedSchemas = new ArrayList<>();

        IntStream.range(0, 3).forEach(index -> {
                    String schemaId = String.valueOf(index);
                    String avroString = getResource(RESOURCE_PATH + "/" + OFFENDERS_CONTRACT);
                    val schemaResponse = new S3SchemaResponse(schemaId, avroString, VERSION_ID);
                    expectedSchemas.add(schemaResponse);
                }
        );

        when(client.getAllSchemas(any())).thenReturn(expectedSchemas);

        List<SourceReference> result = underTest.getAllSourceReferences(schemaGroup);

        List<String> actualIds = result
                .stream()
                .map(SourceReference::getKey)
                .collect(Collectors.toList());

        List<String> expectedIds = expectedSchemas.stream().map(S3SchemaResponse::getId).collect(Collectors.toList());

        assertThat(actualIds, containsTheSameElementsInOrderAs(expectedIds));
    }

    @Test
    public void shouldReturnAnEmptyListWhenNoSchemaIsFound() {
        ImmutableSet<ImmutablePair<String, String>> schemaGroup = ImmutableSet
                .of(ImmutablePair.of("test_schema", "test_table"));

        when(client.getAllSchemas(any())).thenReturn(Collections.emptyList());

        assertThat((Collection<SourceReference>) underTest.getAllSourceReferences(schemaGroup), is(empty()));
    }
}