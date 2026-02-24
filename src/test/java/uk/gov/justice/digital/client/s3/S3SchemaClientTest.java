package uk.gov.justice.digital.client.s3;

import com.google.common.collect.ImmutableSet;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hamcrest.core.Is;
import org.hamcrest.core.IsEqual;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import uk.gov.justice.digital.config.JobArguments;

import java.io.ByteArrayInputStream;
import java.util.Optional;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.any;
import static uk.gov.justice.digital.client.s3.S3SchemaClient.S3SchemaResponse;
import static uk.gov.justice.digital.client.s3.S3SchemaClient.SCHEMA_FILE_EXTENSION;
import static uk.gov.justice.digital.config.JobArguments.SCHEMA_CACHE_MAX_SIZE_DEFAULT;
import static uk.gov.justice.digital.config.JobArguments.SCHEMA_CACHE_EXPIRY_IN_MINUTES_DEFAULT;
import static uk.gov.justice.digital.test.TestHelpers.givenConfiguredRetriesJobArgs;

@ExtendWith(MockitoExtension.class)
class S3SchemaClientTest {

    private static final String SCHEMA_NAME = "some_source/some_table";
    private static final String FAKE_SCHEMA_DEFINITION = "This is a fake schema definition";
    private static final String VERSION_ID = UUID.randomUUID().toString();
    private static final String SCHEMA_REGISTRY = "test-contract-registry";
    private static final Integer MAX_OBJECTS_PER_PAGE = 10;

    @Mock
    private S3ClientProvider mockClientProvider;

    @Mock
    private S3Client mockClient;

    @Mock
    private JobArguments mockArguments;

    @Mock
    private ListObjectsV2Response mockListObjectsV2Response;

    @Captor
    ArgumentCaptor<ListObjectsV2Request> listObjectsV2RequestCaptor;

    private S3SchemaClient underTest;

    @BeforeEach
    void setup() {
        reset(mockClientProvider, mockClient, mockArguments, mockListObjectsV2Response);
        givenSuccessfulJobArgumentCalls();
        givenClientProviderReturnsAClient();
        givenConfiguredRetriesJobArgs(1, mockArguments);
        underTest = new S3SchemaClient(mockClientProvider, mockArguments);
    }

    @Test
    void shouldReturnSchemaForValidRequest() {
        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, SCHEMA_NAME + SCHEMA_FILE_EXTENSION);

        val result = underTest.getSchema(SCHEMA_NAME);

        assertEquals(Optional.of(new S3SchemaResponse(SCHEMA_NAME, FAKE_SCHEMA_DEFINITION, VERSION_ID)), result);
    }

    @Test
    @SuppressWarnings({"unchecked"})
    void shouldUseCachedSchemaWhenOneExists() {
        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, SCHEMA_NAME + SCHEMA_FILE_EXTENSION);

        val firstResult = underTest.getSchema(SCHEMA_NAME);

        assertEquals(Optional.of(new S3SchemaResponse(SCHEMA_NAME, FAKE_SCHEMA_DEFINITION, VERSION_ID)), firstResult);

        val secondResult = underTest.getSchema(SCHEMA_NAME);

        assertEquals(firstResult, secondResult);

        verify(mockClient, times(1)).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
    }

    @Test
    @SuppressWarnings({"unchecked"})
    void shouldEvictOldItemsWhenCacheIsFull() {
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

        verify(mockClient, times(3)).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
    }

    @Test
    void shouldReturnAnEmptyOptionalForASchemaThatDoesNotExist() {
        when(mockClient.getObject(any(GetObjectRequest.class)))
                .thenThrow(SdkClientException.builder().message("Schema not found").build());

        val result = underTest.getSchema(SCHEMA_NAME);

        assertFalse(result.isPresent());
    }

    @Test
    @SuppressWarnings({"unchecked"})
    void shouldRetryWhenAnAmazonClientExceptionOccursWhenRetrievingSchema() {
        int numRetries = 2;
        givenConfiguredRetriesJobArgs(numRetries, mockArguments);
        when(mockClient.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
                .thenThrow(SdkClientException.builder().message("Client error").build());

        S3SchemaClient s3SchemaClient = new S3SchemaClient(mockClientProvider, mockArguments);

        assertThat(s3SchemaClient.getSchema(SCHEMA_NAME), is(Optional.empty()));

        verify(mockClient, times(numRetries)).getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));
    }

    @Test
    void shouldRetrieveAllSchemas() {
        List<ImmutablePair<String, String>> schemaNamesList = new ArrayList<>();
        schemaNamesList.add(ImmutablePair.of("schema1", "some_table"));
        schemaNamesList.add(ImmutablePair.of("schema2", "some_table"));
        schemaNamesList.add(ImmutablePair.of("schema3", "some_table"));
        val schemaNames = ImmutableSet.copyOf(schemaNamesList);

        givenListObjectsV2ResultSucceeds(createObjects(schemaNames));
        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, "schema1/some_table" + SCHEMA_FILE_EXTENSION);
        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, "schema2/some_table" + SCHEMA_FILE_EXTENSION);
        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, "schema3/some_table" + SCHEMA_FILE_EXTENSION);

        val result = underTest.getAllSchemas(schemaNames);

        assertThat(listObjectsV2RequestCaptor.getValue().maxKeys(), Is.is(IsEqual.equalTo(MAX_OBJECTS_PER_PAGE)));
        assertThat(result.size(), equalTo(schemaNames.size()));
    }

    @Test
    void shouldRetrieveAllSchemasWhenObjectsListExceedsOnePage() {
        List<ImmutablePair<String, String>> firstPageSchemaNamesList = new ArrayList<>();
        firstPageSchemaNamesList.add(ImmutablePair.of("schema1", "some_table"));
        firstPageSchemaNamesList.add(ImmutablePair.of("schema2", "some_table"));
        firstPageSchemaNamesList.add(ImmutablePair.of("schema3", "some_table"));
        val firstPageSchemaNames = ImmutableSet.copyOf(firstPageSchemaNamesList);

        List<ImmutablePair<String, String>> secondPageSchemaNamesList = new ArrayList<>();
        secondPageSchemaNamesList.add(ImmutablePair.of("schema4", "some_table"));
        secondPageSchemaNamesList.add(ImmutablePair.of("schema5", "some_table"));
        secondPageSchemaNamesList.add(ImmutablePair.of("schema6", "some_table"));
        val secondPageSchemaNames = ImmutableSet.copyOf(secondPageSchemaNamesList);

        val allSchemaList = Stream.concat(firstPageSchemaNamesList.stream(), secondPageSchemaNamesList.stream())
                .toList();
        val allSchemaNames = ImmutableSet.copyOf(allSchemaList);

        givenMultiPageListObjectsV2ResultSucceeds(createObjects(firstPageSchemaNames), createObjects(secondPageSchemaNames));

        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, "schema1/some_table" + SCHEMA_FILE_EXTENSION);
        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, "schema2/some_table" + SCHEMA_FILE_EXTENSION);
        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, "schema3/some_table" + SCHEMA_FILE_EXTENSION);

        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, "schema4/some_table" + SCHEMA_FILE_EXTENSION);
        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, "schema5/some_table" + SCHEMA_FILE_EXTENSION);
        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, "schema6/some_table" + SCHEMA_FILE_EXTENSION);

        val result = underTest.getAllSchemas(allSchemaNames);

        assertThat(listObjectsV2RequestCaptor.getValue().maxKeys(), Is.is(IsEqual.equalTo(MAX_OBJECTS_PER_PAGE)));
        assertThat(result.size(), is(equalTo(allSchemaNames.size())));
    }

    @Test
    void shouldReturnAnEmptyListWhenThereAreNoSchemas() {
        val schemaGroup = ImmutableSet.copyOf(Collections.singleton(ImmutablePair.of("test_schema", "test_table")));

        givenObjectListIsEmpty();

        assertThat((Collection<S3SchemaResponse>) underTest.getAllSchemas(schemaGroup), is(empty()));
    }

    @Test
    void shouldFailWhenThereIsAMissingSchema() {
        ImmutableSet<ImmutablePair<String, String>> schemaNames = ImmutableSet.of(
                ImmutablePair.of("schema1", "some_table"),
                ImmutablePair.of("schema2", "some_table")
        );

        givenListObjectsV2ResultSucceeds(createObjects(schemaNames));
        givenSchemaRetrievalSucceeds(FAKE_SCHEMA_DEFINITION, "schema1/some_table" + SCHEMA_FILE_EXTENSION);
        when(mockClient.getObject(any(GetObjectRequest.class)))
                .thenThrow(SdkClientException.builder().message("Schema not found").build());

        assertThrows(RuntimeException.class, () -> underTest.getAllSchemas(schemaNames));
    }

    @Test
    void shouldThrowAnExceptionIfAnErrorOccursWhenListingSchemas() {
        ImmutableSet<ImmutablePair<String, String>> schemaGroup = ImmutableSet
                .of(ImmutablePair.of("test_schema", "test_table"));

        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenThrow(SdkClientException.builder().message("failed to list schemas").build());

        assertThrows(SdkClientException.class, () -> underTest.getAllSchemas(schemaGroup));
    }

    private void givenClientProviderReturnsAClient() {
        when(mockClientProvider.getClient()).thenReturn(mockClient);
    }

    private void givenSuccessfulJobArgumentCalls() {
        when(mockArguments.getContractRegistryName()).thenReturn(SCHEMA_REGISTRY);
        when(mockArguments.getSchemaCacheMaxSize()).thenReturn(SCHEMA_CACHE_MAX_SIZE_DEFAULT);
        when(mockArguments.getSchemaCacheExpiryInMinutes()).thenReturn(SCHEMA_CACHE_EXPIRY_IN_MINUTES_DEFAULT);
        when(mockArguments.getMaxObjectsPerPage()).thenReturn(MAX_OBJECTS_PER_PAGE);
    }

    @SuppressWarnings({"unchecked"})
    private void givenSchemaRetrievalSucceeds(String schemaDefinition, String objectKey) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(schemaDefinition.getBytes());
        GetObjectResponse getObjectResponse = GetObjectResponse.builder().versionId(VERSION_ID).build();
        ResponseInputStream<GetObjectResponse> response = new ResponseInputStream<>(getObjectResponse, inputStream);

        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(SCHEMA_REGISTRY)
                .key(objectKey)
                .build();

        when(mockClient.getObject(eq(request), any(ResponseTransformer.class))).thenReturn(response);
    }

    private void givenListObjectsV2ResultSucceeds(List<S3Object> objectSummaries) {
        when(mockListObjectsV2Response.contents()).thenReturn(objectSummaries);
        when(mockListObjectsV2Response.isTruncated()).thenReturn(false);
        when(mockClient.listObjectsV2(listObjectsV2RequestCaptor.capture())).thenReturn(mockListObjectsV2Response);
    }

    @SuppressWarnings({"unchecked", "varargs"})
    private void givenMultiPageListObjectsV2ResultSucceeds(List<S3Object> firstPage, List<S3Object> secondPage) {
        when(mockListObjectsV2Response.contents()).thenReturn(firstPage, secondPage);
        when(mockListObjectsV2Response.isTruncated()).thenReturn(true, false);
        when(mockClient.listObjectsV2(listObjectsV2RequestCaptor.capture())).thenReturn(mockListObjectsV2Response);
    }

    private void givenObjectListIsEmpty() {
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(mockListObjectsV2Response);
        when(mockListObjectsV2Response.contents()).thenReturn(Collections.emptyList());
    }

    @NotNull
    private static List<S3Object> createObjects(ImmutableSet<ImmutablePair<String, String>> schemaNames) {
        return schemaNames.stream()
                .map(schemaName -> S3Object.builder()
                        .key(schemaName.getLeft() + "/" + schemaName.getRight() + SCHEMA_FILE_EXTENSION)
                        .build()).toList();
    }

}
