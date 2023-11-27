package uk.gov.justice.digital.client.glue;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.*;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.glue.GlueSchemaClient.GlueSchemaResponse;
import uk.gov.justice.digital.config.JobArguments;

import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.test.SparkTestHelpers.containsTheSameElementsInOrderAs;

@ExtendWith(MockitoExtension.class)
public class GlueSchemaClientTest {

    private static final String SCHEMA_NAME = "somesource.sometable";
    private static final String FAKE_SCHEMA_DEFINITION = "This is a fake schema definition";
    private static final String FIXED_UUID = "35AC2858-B6B4-462A-8F5E-A13D6E9E0FF2";

    @Mock
    private GlueClientProvider mockClientProvider;

    @Mock
    private AWSGlue mockClient;

    @Mock
    private JobArguments mockArguments;

    @Mock
    private GetSchemaVersionResult mockResponse;

    @Captor
    ArgumentCaptor<ListSchemasRequest> listSchemaRequestCaptor;

    @Captor
    ArgumentCaptor<GetSchemaVersionRequest> schemaVersionRequestCaptor;

    private GlueSchemaClient underTest;

    @BeforeEach
    public void setup() {
        givenJobArgumentsReturnAContractRegistryName();
        givenClientProviderReturnsAClient();
        underTest = new GlueSchemaClient(mockClientProvider, mockArguments);
    }

    @Test
    public void shouldReturnASchemaForAValidRequest() {
        givenClientReturnsAValidResponse();

        val result = underTest.getSchema(SCHEMA_NAME);

        assertEquals(Optional.of(new GlueSchemaResponse(FIXED_UUID, FAKE_SCHEMA_DEFINITION)), result);
    }

    @Test
    public void shouldReturnAnEmptyOptionalForASchemaThatDoesNotExist() {
        givenClientThrowsAnEntityNotFoundException();

        val result = underTest.getSchema(SCHEMA_NAME);

        assertFalse(result.isPresent());
    }

    @Test
    public void shouldThrowAnExceptionIfAnyOtherErrorOccurs() {
        givenClientThrowsSomeOtherException();

        assertThrows(SdkClientException.class, () -> underTest.getSchema(SCHEMA_NAME));
    }

    @Test
    public void shouldRetrieveAllSchemas() {
        List<String> schemaNames = new ArrayList<>();
        schemaNames.add("schema_1");
        schemaNames.add("schema_2");
        schemaNames.add("schema_3");

        val schemas = schemaNames.stream()
                .map(schemaName -> new SchemaListItem().withSchemaName(schemaName))
                .collect(Collectors.toList());

        ListSchemasResult listSchemasResult = new ListSchemasResult().withSchemas(schemas);

        when(mockClient.listSchemas(listSchemaRequestCaptor.capture())).thenReturn(listSchemasResult);
        when(mockResponse.getSchemaVersionId()).thenReturn(FIXED_UUID);
        when(mockResponse.getSchemaDefinition()).thenReturn(FAKE_SCHEMA_DEFINITION);
        when(mockClient.getSchemaVersion(schemaVersionRequestCaptor.capture())).thenReturn(mockResponse);

        val result = underTest.getAllSchemas();

        List<String> actualSchemaNames = schemaVersionRequestCaptor.getAllValues()
                .stream()
                .map(schemaVersionRequest -> schemaVersionRequest.getSchemaId().getSchemaName())
                .collect(Collectors.toList());

        assertThat(
                actualSchemaNames,
                containsTheSameElementsInOrderAs(schemaNames)
        );

        int expectedPageSize = 100;
        assertThat(listSchemaRequestCaptor.getValue().getMaxResults(), equalTo(expectedPageSize));

        assertThat(result.size(), equalTo(schemaNames.size()));
    }

    @Test
    public void shouldReturnAnEmptyListWhenThereAreNoSchemas() {
        ListSchemasResult emptyListSchemasResult = new ListSchemasResult().withSchemas(Collections.emptyList());
        when(mockClient.listSchemas(any())).thenReturn(emptyListSchemasResult);

        assertThat((Collection<GlueSchemaResponse>) underTest.getAllSchemas(), is(empty()));
    }

    @Test
    public void shouldFailWhenThereIsAMissingSchema() {
        List<String> schemaNames = new ArrayList<>();
        schemaNames.add("schema_1");
        schemaNames.add("schema_2");
        schemaNames.add("schema_3");

        val schemas = schemaNames.stream()
                .map(schemaName -> new SchemaListItem().withSchemaName(schemaName))
                .collect(Collectors.toList());

        ListSchemasResult listSchemasResult = new ListSchemasResult().withSchemas(schemas);

        when(mockClient.listSchemas(listSchemaRequestCaptor.capture())).thenReturn(listSchemasResult);
        when(mockClient.getSchemaVersion(any())).thenThrow(new EntityNotFoundException("Schema not found"));

        assertThrows(RuntimeException.class, () -> underTest.getAllSchemas());
    }

    @Test
    public void shouldThrowAnExceptionIfAnErrorOccursWhenListingSchemas() {
        when(mockClient.listSchemas(any())).thenThrow(new AWSGlueException("failed to list schemas"));

        assertThrows(AWSGlueException.class, () -> underTest.getAllSchemas());
    }

    private void givenClientProviderReturnsAClient() {
        when(mockClientProvider.getClient()).thenReturn(mockClient);
    }

    private void givenJobArgumentsReturnAContractRegistryName() {
        when(mockArguments.getContractRegistryName()).thenReturn("test-contract-registry");
    }

    private void givenClientReturnsAValidResponse() {
        when(mockResponse.getSchemaVersionId()).thenReturn(FIXED_UUID);
        when(mockResponse.getSchemaDefinition()).thenReturn(FAKE_SCHEMA_DEFINITION);
        when(mockClient.getSchemaVersion(any())).thenReturn(mockResponse);
    }

    private void givenClientThrowsAnEntityNotFoundException() {
        when(mockClient.getSchemaVersion(any())).thenThrow(new EntityNotFoundException("Schema not found"));
    }

    private void givenClientThrowsSomeOtherException() {
        when(mockClient.getSchemaVersion(any())).thenThrow(new SdkClientException("Some other client error"));
    }

}