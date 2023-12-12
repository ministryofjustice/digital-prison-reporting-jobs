package uk.gov.justice.digital.client.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.client.s3.S3SchemaClient.S3SchemaResponse;
import static uk.gov.justice.digital.client.s3.S3SchemaClient.SCHEMA_FILE_EXTENSION;
import static uk.gov.justice.digital.test.SparkTestHelpers.containsTheSameElementsInOrderAs;

@ExtendWith(MockitoExtension.class)
public class S3SchemaClientTest {

    private static final String SCHEMA_NAME = "some_source/some_table";
    private static final String FAKE_SCHEMA_DEFINITION = "This is a fake schema definition";
    private static final String VERSION_ID = UUID.randomUUID().toString();
    private static final String SCHEMA_REGISTRY = "test-contract-registry";

    @Mock
    private S3SchemaClientProvider mockClientProvider;

    @Mock
    private AmazonS3 mockClient;

    @Mock
    private JobArguments mockArguments;

    @Mock
    private ObjectListing mockObjectListing;

    @Mock
    private S3Object mockS3Object;

    @Mock
    private ObjectMetadata mockObjectMetadata;

    @Captor
    ArgumentCaptor<ListObjectsRequest> listObjectsRequestCaptor;

    @Captor
    ArgumentCaptor<String> schemaRequestCaptor;

    private S3SchemaClient underTest;

    @BeforeEach
    public void setup() {
        givenJobArgumentsReturnsContractRegistryName();
        givenClientProviderReturnsAClient();
        underTest = new S3SchemaClient(mockClientProvider, mockArguments);
    }

    @Test
    public void shouldReturnSchemaForValidRequest() {
        givenSchemaRetrievalSucceeds();
        when(mockClient.getObject(SCHEMA_REGISTRY, SCHEMA_NAME + SCHEMA_FILE_EXTENSION)).thenReturn(mockS3Object);

        val result = underTest.getSchema(SCHEMA_NAME);

        assertEquals(Optional.of(new S3SchemaResponse(SCHEMA_NAME, FAKE_SCHEMA_DEFINITION, VERSION_ID)), result);
    }

    @Test
    public void shouldReturnAnEmptyOptionalForASchemaThatDoesNotExist() {
        when(mockClient.getObject(anyString(), anyString())).thenThrow(new AmazonClientException("Schema not found"));

        val result = underTest.getSchema(SCHEMA_NAME);

        assertFalse(result.isPresent());
    }

    @Test
    public void shouldRetrieveAllSchemas() {
        List<String> schemaNames = new ArrayList<>();
        schemaNames.add("schema1/some_table");
        schemaNames.add("schema2/some_table");
        schemaNames.add("schema3/some_table");

        givenObjectListingSucceeds(createObjectSummaries(schemaNames));
        givenSchemaRetrievalSucceeds();
        when(mockClient.getObject(eq(SCHEMA_REGISTRY), schemaRequestCaptor.capture())).thenReturn(mockS3Object);

        val result = underTest.getAllSchemas(new HashSet<>(schemaNames));

        List<String> expectedSchemaRequestNames = schemaNames.stream()
                .map(item -> item + SCHEMA_FILE_EXTENSION)
                .collect(Collectors.toList());
        assertThat(schemaRequestCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedSchemaRequestNames));

        assertThat(listObjectsRequestCaptor.getValue().getBucketName(), equalTo(SCHEMA_REGISTRY));

        assertThat(result.size(), equalTo(schemaNames.size()));
    }

    @Test
    public void shouldReturnAnEmptyListWhenThereAreNoSchemas() {
        Set<String> schemaGroup = Collections.singleton("test_schema/test_table");

        givenObjectListIsEmpty();

        assertThat((Collection<S3SchemaResponse>) underTest.getAllSchemas(schemaGroup), is(empty()));
    }

    @Test
    public void shouldFailWhenThereIsAMissingSchema() {
        List<String> schemaNames = new ArrayList<>();
        schemaNames.add("schema1/some_table");
        schemaNames.add("schema2/some_table");

        givenObjectListingSucceeds(createObjectSummaries(schemaNames));
        givenSchemaRetrievalSucceeds();
        when(mockClient.getObject(SCHEMA_REGISTRY, "schema1/some_table" + SCHEMA_FILE_EXTENSION))
                .thenReturn(mockS3Object);
        when(mockClient.getObject(SCHEMA_REGISTRY, "schema2/some_table" + SCHEMA_FILE_EXTENSION))
                .thenThrow(new AmazonClientException("Schema not found"));

        assertThrows(RuntimeException.class, () -> underTest.getAllSchemas(new HashSet<>(schemaNames)));
    }

    @Test
    public void shouldThrowAnExceptionIfAnErrorOccursWhenListingSchemas() {
        Set<String> schemaGroup = Collections.singleton("test_schema/test_table");

        when(mockClient.listObjects(any(ListObjectsRequest.class))).thenThrow(new AmazonClientException("failed to list schemas"));

        assertThrows(AmazonClientException.class, () -> underTest.getAllSchemas(schemaGroup));
    }

    private void givenClientProviderReturnsAClient() {
        when(mockClientProvider.getClient()).thenReturn(mockClient);
    }

    private void givenJobArgumentsReturnsContractRegistryName() {
        when(mockArguments.getContractRegistryName()).thenReturn(SCHEMA_REGISTRY);
    }

    @NotNull
    private static List<S3ObjectSummary> createObjectSummaries(List<String> schemaNames) {
        return schemaNames.stream()
                .map(schemaName -> {
                    S3ObjectSummary objectSummary = new S3ObjectSummary();
                    objectSummary.setKey(schemaName + SCHEMA_FILE_EXTENSION);
                    return objectSummary;
                }).collect(Collectors.toList());
    }

    private void givenSchemaRetrievalSucceeds() {
        when(mockS3Object.getObjectMetadata()).thenReturn(mockObjectMetadata);
        when(mockObjectMetadata.getVersionId()).thenReturn(VERSION_ID);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(FAKE_SCHEMA_DEFINITION.getBytes());
        when(mockS3Object.getObjectContent()).thenReturn(new S3ObjectInputStream(inputStream, null));
    }

    private void givenObjectListingSucceeds(List<S3ObjectSummary> objectSummaries) {
        when(mockObjectListing.getObjectSummaries()).thenReturn(objectSummaries);
        when(mockObjectListing.isTruncated()).thenReturn(false);
        when(mockClient.listObjects(listObjectsRequestCaptor.capture())).thenReturn(mockObjectListing);
    }

    private void givenObjectListIsEmpty() {
        when(mockClient.listObjects(any(ListObjectsRequest.class))).thenReturn(mockObjectListing);
        when(mockObjectListing.getObjectSummaries()).thenReturn(Collections.emptyList());
    }

}