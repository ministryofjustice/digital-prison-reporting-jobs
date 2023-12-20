package uk.gov.justice.digital.client.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.*;
import com.google.common.collect.ImmutableSet;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
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
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.client.s3.S3SchemaClient.S3SchemaResponse;
import static uk.gov.justice.digital.client.s3.S3SchemaClient.SCHEMA_FILE_EXTENSION;
import static uk.gov.justice.digital.config.JobArguments.SCHEMA_CACHE_EXPIRY_IN_MINUTES_DEFAULT;
import static uk.gov.justice.digital.config.JobArguments.SCHEMA_CACHE_MAX_SIZE_DEFAULT;

@ExtendWith(MockitoExtension.class)
public class S3SchemaClientTest {

    private static final String SCHEMA_NAME = "some_source/some_table";
    private static final String FAKE_SCHEMA_DEFINITION = "This is a fake schema definition";
    private static final String VERSION_ID = UUID.randomUUID().toString();
    private static final String SCHEMA_REGISTRY = "test-contract-registry";

    @Mock
    private S3ClientProvider mockClientProvider;

    @Mock
    private AmazonS3 mockClient;

    @Mock
    private JobArguments mockArguments;

    @Mock
    private ObjectListing mockObjectListing;

    @Captor
    ArgumentCaptor<ListObjectsRequest> listObjectsRequestCaptor;

    private S3SchemaClient underTest;

    @BeforeEach
    public void setup() {
        reset(mockClientProvider, mockClient, mockArguments, mockObjectListing);
        givenSuccessfulJobArgumentCalls();
        givenClientProviderReturnsAClient();
        underTest = new S3SchemaClient(mockClientProvider, mockArguments);
    }

    @Test
    public void shouldReturnSchemaForValidRequest() {
        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, SCHEMA_NAME + SCHEMA_FILE_EXTENSION);

        val result = underTest.getSchema(SCHEMA_NAME);

        assertEquals(Optional.of(new S3SchemaResponse(SCHEMA_NAME, FAKE_SCHEMA_DEFINITION, VERSION_ID)), result);
    }

    @Test
    public void shouldUseCachedSchemaWhenOneExists() {
        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, SCHEMA_NAME + SCHEMA_FILE_EXTENSION);

        val firstResult = underTest.getSchema(SCHEMA_NAME);

        assertEquals(Optional.of(new S3SchemaResponse(SCHEMA_NAME, FAKE_SCHEMA_DEFINITION, VERSION_ID)), firstResult);

        val secondResult = underTest.getSchema(SCHEMA_NAME);

        assertEquals(firstResult, secondResult);

        verify(mockClient, times(1)).getObject(anyString(), anyString());
    }

    @Test
    public void shouldEvictOldItemsWhenCacheIsFull() {
        when(mockArguments.getSchemaCacheMaxSize()).thenReturn(1L);
        String secondSchemaName = SCHEMA_NAME + "-new";
        String secondSchemaDefinition = FAKE_SCHEMA_DEFINITION + "-new";
        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, SCHEMA_NAME + SCHEMA_FILE_EXTENSION);
        givenSchemaRetrievalSucceeds(secondSchemaDefinition, secondSchemaName + SCHEMA_FILE_EXTENSION);

        S3SchemaClient schemaClient = new S3SchemaClient(mockClientProvider, mockArguments);

        val firstResult = schemaClient.getSchema(SCHEMA_NAME);
        val secondResult = schemaClient.getSchema(secondSchemaName);

        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, SCHEMA_NAME + SCHEMA_FILE_EXTENSION);
        val thirdResult = schemaClient.getSchema(SCHEMA_NAME);

        assertEquals(Optional.of(new S3SchemaResponse(SCHEMA_NAME, FAKE_SCHEMA_DEFINITION, VERSION_ID)), firstResult);
        assertEquals(Optional.of(new S3SchemaResponse(secondSchemaName, secondSchemaDefinition, VERSION_ID)), secondResult);
        assertEquals(firstResult, thirdResult);

        verify(mockClient, times(3)).getObject(anyString(), anyString());
    }

    @Test
    public void shouldReturnAnEmptyOptionalForASchemaThatDoesNotExist() {
        when(mockClient.getObject(anyString(), anyString())).thenThrow(new AmazonClientException("Schema not found"));

        val result = underTest.getSchema(SCHEMA_NAME);

        assertFalse(result.isPresent());
    }

    @Test
    public void shouldRetrieveAllSchemas() {
        List<ImmutablePair<String, String>> schemaNamesList = new ArrayList<>();
        schemaNamesList.add(ImmutablePair.of("schema1", "some_table"));
        schemaNamesList.add(ImmutablePair.of("schema2", "some_table"));
        schemaNamesList.add(ImmutablePair.of("schema3", "some_table"));
        val schemaNames = ImmutableSet.copyOf(schemaNamesList);

        givenObjectListingSucceeds(createObjectSummaries(schemaNames));
        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, "schema1/some_table" + SCHEMA_FILE_EXTENSION);
        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, "schema2/some_table" + SCHEMA_FILE_EXTENSION);
        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, "schema3/some_table" + SCHEMA_FILE_EXTENSION);

        val result = underTest.getAllSchemas(schemaNames);

        assertThat(result.size(), equalTo(schemaNames.size()));
    }

    @Test
    public void shouldReturnAnEmptyListWhenThereAreNoSchemas() {
        val schemaGroup = ImmutableSet.copyOf(Collections.singleton(ImmutablePair.of("test_schema", "test_table")));

        givenObjectListIsEmpty();

        assertThat((Collection<S3SchemaResponse>) underTest.getAllSchemas(schemaGroup), is(empty()));
    }

    @Test
    public void shouldFailWhenThereIsAMissingSchema() {
        ImmutableSet<ImmutablePair<String, String>> schemaNames = ImmutableSet.of(
                ImmutablePair.of("schema1", "some_table"),
                ImmutablePair.of("schema2", "some_table")
        );

        givenObjectListingSucceeds(createObjectSummaries(schemaNames));
        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, "schema1/some_table" + SCHEMA_FILE_EXTENSION);
        when(mockClient.getObject(SCHEMA_REGISTRY, "schema2/some_table" + SCHEMA_FILE_EXTENSION))
                .thenThrow(new AmazonClientException("Schema not found"));

        assertThrows(RuntimeException.class, () -> underTest.getAllSchemas(schemaNames));
    }

    @Test
    public void shouldThrowAnExceptionIfAnErrorOccursWhenListingSchemas() {
        ImmutableSet<ImmutablePair<String, String>> schemaGroup = ImmutableSet
                .of(ImmutablePair.of("test_schema", "test_table"));

        when(mockClient.listObjects(any(ListObjectsRequest.class))).thenThrow(new AmazonClientException("failed to list schemas"));

        assertThrows(AmazonClientException.class, () -> underTest.getAllSchemas(schemaGroup));
    }

    private void givenClientProviderReturnsAClient() {
        when(mockClientProvider.getClient()).thenReturn(mockClient);
    }

    private void givenSuccessfulJobArgumentCalls() {
        when(mockArguments.getContractRegistryName()).thenReturn(SCHEMA_REGISTRY);
        when(mockArguments.getSchemaCacheMaxSize()).thenReturn(SCHEMA_CACHE_MAX_SIZE_DEFAULT);
        when(mockArguments.getSchemaCacheExpiryInMinutes()).thenReturn(SCHEMA_CACHE_EXPIRY_IN_MINUTES_DEFAULT);
    }

    private void givenSchemaRetrievalSucceeds(String schemaDefinition, String objectKey) {
        val objectMetadata = new ObjectMetadata();
        objectMetadata.setHeader(Headers.S3_VERSION_ID, VERSION_ID);

        val schemaObject = new S3Object();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(schemaDefinition.getBytes());
        schemaObject.setObjectContent(inputStream);
        schemaObject.setObjectMetadata(objectMetadata);

        when(mockClient.getObject(SCHEMA_REGISTRY, objectKey)).thenReturn(schemaObject);
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

    @NotNull
    private static List<S3ObjectSummary> createObjectSummaries(ImmutableSet<ImmutablePair<String, String>> schemaNames) {
        return schemaNames.stream()
                .map(schemaName -> {
                    S3ObjectSummary objectSummary = new S3ObjectSummary();
                    objectSummary.setKey(schemaName.getLeft() + "/" + schemaName.getRight() + SCHEMA_FILE_EXTENSION);
                    return objectSummary;
                }).collect(Collectors.toList());
    }

}