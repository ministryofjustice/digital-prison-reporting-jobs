package uk.gov.justice.digital.client.s3;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.entity.ContentType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.S3Object;
import uk.gov.justice.digital.config.JobArguments;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.any;
import static uk.gov.justice.digital.test.Fixtures.fixedClock;
import static uk.gov.justice.digital.common.RegexPatterns.matchAllFiles;
import static uk.gov.justice.digital.common.RegexPatterns.parquetFileRegex;
import static uk.gov.justice.digital.common.RegexPatterns.jsonOrParquetFileRegex;

@ExtendWith(MockitoExtension.class)
class S3ClientTest {

    @Mock
    S3ClientProvider mockS3ClientProvider;
    @Mock
    S3Client mockS3Client;
    @Mock
    ListObjectsV2Response listObjectsV2Response;
    @Mock
    JobArguments mockJobArgs;
    @Mock
    DeleteObjectsResponse mockDeleteObjectsResponse;
    @Captor
    ArgumentCaptor<GetObjectRequest> getObjectRequestCaptor;
    @Captor
    ArgumentCaptor<ListObjectsV2Request> listObjectsV2RequestCaptor;
    @Captor
    ArgumentCaptor<DeleteObjectsRequest> deleteObjectsRequestCaptor;
    @Captor
    ArgumentCaptor<PutObjectRequest> putObjectRequestCaptor;
    @Captor
    ArgumentCaptor<RequestBody> requestBodyCaptor;
    @Captor
    ArgumentCaptor<CopyObjectRequest> copyObjectRequestCaptor;

    private static final String SOURCE_KEY = "test-source-key";
    private static final String DESTINATION_KEY = "test-destination-key";
    private static final String SOURCE_BUCKET = "test-source-bucket";
    private static final String DESTINATION_BUCKET = "test-destination-bucket";
    private static final String TEST_FOLDER = "test-folder";
    private static final Integer MAX_OBJECTS_PER_PAGE = 10;
    private static final Duration zeroDayPeriod = Duration.of(0L, ChronoUnit.DAYS);

    private S3ObjectClient underTest;

    @BeforeEach
    void setUp() {
        reset(mockS3ClientProvider, mockS3Client, listObjectsV2Response, mockJobArgs, mockDeleteObjectsResponse);

        when(mockS3ClientProvider.getClient()).thenReturn(mockS3Client);
        when(mockJobArgs.getMaxObjectsPerPage()).thenReturn(MAX_OBJECTS_PER_PAGE);
        underTest = new S3ObjectClient(mockS3ClientProvider, mockJobArgs);
    }

    @Test
    @SuppressWarnings({"unchecked"})
    void getObjectShouldRetrieveObject() throws IOException {
        InputStream stream = new ByteArrayInputStream("{}".getBytes(StandardCharsets.UTF_8));
        ResponseInputStream<GetObjectResponse> response = new ResponseInputStream<>(GetObjectResponse.builder().build(), stream);
        when(mockS3Client.getObject(getObjectRequestCaptor.capture(), any(ResponseTransformer.class)))
                .thenReturn(response);

        underTest.getObject(SOURCE_BUCKET, SOURCE_KEY);

        GetObjectRequest getObjectRequest = getObjectRequestCaptor.getValue();
        assertEquals(SOURCE_BUCKET, getObjectRequest.bucket());
        assertEquals(SOURCE_KEY, getObjectRequest.key());
    }

    @Test
    @SuppressWarnings({"unchecked"})
    void getObjectShouldFailWhenClientThrowsAnException() {
        doThrow(new RuntimeException("client exception")).when(mockS3Client)
                .getObject(any(GetObjectRequest.class), any(ResponseTransformer.class));

        assertThrows(RuntimeException.class, () -> underTest.getObject(SOURCE_BUCKET, SOURCE_KEY));
    }

    @Test
    void saveObjectShouldSaveGivenDataBytes() throws IOException {
        String dataString = "{}";
        byte[] data = dataString.getBytes(StandardCharsets.UTF_8);
        when(mockS3Client.putObject(putObjectRequestCaptor.capture(), requestBodyCaptor.capture()))
                .thenReturn(PutObjectResponse.builder().build());

        underTest.saveObject(SOURCE_BUCKET, SOURCE_KEY, data, ContentType.DEFAULT_TEXT);

        PutObjectRequest request = putObjectRequestCaptor.getValue();
        assertEquals(SOURCE_BUCKET, request.bucket());
        assertEquals(SOURCE_KEY, request.key());
        assertEquals(Long.valueOf(data.length), request.contentLength());
        assertEquals(ContentType.DEFAULT_TEXT.getMimeType(), request.contentType());

        RequestBody requestBody = requestBodyCaptor.getValue();

        String actualDataString = IOUtils.toString(requestBody.contentStreamProvider().newStream(), StandardCharsets.UTF_8);
        assertEquals(dataString, actualDataString);
    }

    @Test
    void saveObjectShouldFailWhenClientThrowsAnException() {
        byte[] data = new byte[0];
        doThrow(new RuntimeException("client exception"))
                .when(mockS3Client)
                .putObject(any(PutObjectRequest.class), any(RequestBody.class));

        assertThrows(RuntimeException.class, () -> underTest.saveObject(SOURCE_BUCKET, SOURCE_KEY, data, ContentType.DEFAULT_TEXT));
    }

    @Test
    void copyObjectShouldDeleteObjects() {
        when(mockS3Client.copyObject(copyObjectRequestCaptor.capture())).thenReturn(CopyObjectResponse.builder().build());

        underTest.copyObject(SOURCE_KEY, DESTINATION_KEY, SOURCE_BUCKET, DESTINATION_BUCKET);

        CopyObjectRequest copyObjectRequest = copyObjectRequestCaptor.getValue();
        assertEquals(SOURCE_BUCKET, copyObjectRequest.sourceBucket());
        assertEquals(SOURCE_KEY, copyObjectRequest.sourceKey());
        assertEquals(DESTINATION_BUCKET, copyObjectRequest.destinationBucket());
        assertEquals(DESTINATION_KEY, copyObjectRequest.destinationKey());
    }

    @Test
    void copyObjectShouldFailWhenClientThrowsAnException() {
        doThrow(new RuntimeException("client exception")).when(mockS3Client).copyObject(any(CopyObjectRequest.class));

        assertThrows(RuntimeException.class, () -> underTest.copyObject(SOURCE_KEY, DESTINATION_KEY, SOURCE_BUCKET, DESTINATION_BUCKET));
    }

    @Test
    void deleteObjectsShouldDeleteObjectsReturningEmptySetWhenNoObjectsFailed() {
        when(mockDeleteObjectsResponse.deleted()).thenReturn(Collections.emptyList());
        when(mockS3Client.deleteObjects(deleteObjectsRequestCaptor.capture())).thenReturn(mockDeleteObjectsResponse);

        Set<String> failedObjects = underTest.deleteObjects(Collections.singletonList(SOURCE_KEY), SOURCE_BUCKET);

        DeleteObjectsRequest deleteObjectsRequest = deleteObjectsRequestCaptor.getValue();
        List<String> keysToDelete = deleteObjectsRequest
                .delete()
                .objects()
                .stream()
                .map(ObjectIdentifier::key)
                .toList();

        assertThat(failedObjects, is(empty()));
        assertThat(keysToDelete, containsInAnyOrder(SOURCE_KEY));
        assertTrue(deleteObjectsRequest.delete().quiet());
    }

    @Test
    void deleteObjectsShouldDeleteObjectsReturningSetOfFailedKeys() {
        List<String> objectsKeysToDelete = new ArrayList<>();
        objectsKeysToDelete.add("key1");
        objectsKeysToDelete.add("key2");
        objectsKeysToDelete.add("key3");

        DeletedObject deletedObject1 = DeletedObject.builder().key("key1").build();
        DeletedObject deletedObject2 = DeletedObject.builder().key("key2").build();

        List<DeletedObject> deletedObjects = new ArrayList<>();

        deletedObjects.add(deletedObject1);
        deletedObjects.add(deletedObject2);


        when(mockDeleteObjectsResponse.deleted()).thenReturn(deletedObjects);
        when(mockS3Client.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(mockDeleteObjectsResponse);

        Set<String> failedObjects = underTest.deleteObjects(objectsKeysToDelete, SOURCE_BUCKET);

        assertThat(failedObjects, containsInAnyOrder("key1", "key2"));
    }

    @Test
    void deleteObjectsShouldFailWhenClientThrowsAnException() {
        List<String> keysToDelete = Collections.singletonList(SOURCE_KEY);

        doThrow(new RuntimeException("client exception")).when(mockS3Client).deleteObjects(any(DeleteObjectsRequest.class));

        assertThrows(RuntimeException.class, () -> underTest.deleteObjects(keysToDelete, SOURCE_BUCKET));
    }

    @SuppressWarnings("unchecked")
    @Test
    void getObjectsOlderThanShouldReturnListOfObjectsMatchingAllowedExtensionsWithinGivenFolderPrefix() {
        ImmutableSet<ImmutablePair<String, String>> objectKeys = ImmutableSet.of(
                ImmutablePair.of("file1", ".txt"),
                ImmutablePair.of("file2", ".parquet"),
                ImmutablePair.of("file3", ".json"),
                ImmutablePair.of("file4", ".jpg"),
                ImmutablePair.of("file5", ".JSON"),
                ImmutablePair.of("file6", ".PARQUET")
        );

        List<String> expectedObjectKeys = new ArrayList<>();
        expectedObjectKeys.add("file2.parquet");
        expectedObjectKeys.add("file6.PARQUET");
        expectedObjectKeys.add("file3.json");
        expectedObjectKeys.add("file5.JSON");

        Instant lastModifiedDate = Instant.now(fixedClock).minusNanos(1);
        givenObjectListingSucceeds(createObjects(objectKeys, lastModifiedDate));

        List<String> returnedObjectKeys = underTest.getObjectsOlderThan(SOURCE_BUCKET, TEST_FOLDER, jsonOrParquetFileRegex, zeroDayPeriod, fixedClock)
                .stream()
                .map(x -> x.key)
                .toList();

        ListObjectsV2Request listObjectsV2Request = listObjectsV2RequestCaptor.getValue();
        assertThat(listObjectsV2Request.bucket(), is(equalTo(SOURCE_BUCKET)));
        assertThat(listObjectsV2Request.maxKeys(), is(equalTo(MAX_OBJECTS_PER_PAGE)));
        assertThat(listObjectsV2Request.prefix(), is(equalTo(TEST_FOLDER)));
        assertThat(returnedObjectKeys, containsInAnyOrder(expectedObjectKeys.toArray()));
    }

    @Test
    void getObjectsOlderThanShouldReturnListOfObjectsMatchingAllowedExtensionsWhenObjectsListExceedsOnePage() {
        ImmutableSet<ImmutablePair<String, String>> firstSetOfObjectKeys = ImmutableSet.of(
                ImmutablePair.of("file1", ".txt"),
                ImmutablePair.of("file2", ".parquet"),
                ImmutablePair.of("file3", ".json"),
                ImmutablePair.of("file6", ".PARQUET")
        );

        ImmutableSet<ImmutablePair<String, String>> secondSetOfObjectKeys = ImmutableSet.of(
                ImmutablePair.of("file7", ".txt"),
                ImmutablePair.of("file8", ".parquet"),
                ImmutablePair.of("file9", ".json")
        );

        List<String> expectedObjectKeys = new ArrayList<>();
        expectedObjectKeys.add("file2.parquet");
        expectedObjectKeys.add("file6.PARQUET");
        expectedObjectKeys.add("file8.parquet");

        Instant lastModifiedDate = Instant.now(fixedClock).minusNanos(1);
        List<S3Object> firstPageObjects = createObjects(firstSetOfObjectKeys, lastModifiedDate);
        List<S3Object> secondPageObjects = createObjects(secondSetOfObjectKeys, lastModifiedDate);

        givenMultiPageObjectListingSucceeds(firstPageObjects, secondPageObjects);

        List<String> returnedObjectKeys = underTest.getObjectsOlderThan(SOURCE_BUCKET, TEST_FOLDER, parquetFileRegex, zeroDayPeriod, fixedClock)
                .stream()
                .map(x -> x.key)
                .toList();

        ListObjectsV2Request listObjectsV2Request = listObjectsV2RequestCaptor.getValue();
        assertThat(listObjectsV2Request.bucket(), is(equalTo(SOURCE_BUCKET)));
        assertThat(listObjectsV2Request.maxKeys(), is(equalTo(MAX_OBJECTS_PER_PAGE)));
        assertThat(listObjectsV2Request.prefix(), is(equalTo(TEST_FOLDER)));
        assertThat(returnedObjectKeys, containsInAnyOrder(expectedObjectKeys.toArray()));
    }

    @SuppressWarnings("unchecked")
    @Test
    void getObjectsOlderThanShouldReturnListOfAllObjectsWhenGivenWildCardExtension() {
        ImmutableSet<ImmutablePair<String, String>> objectKeys = ImmutableSet.of(
                ImmutablePair.of("file1", ".txt"),
                ImmutablePair.of("file2", ".parquet"),
                ImmutablePair.of("file3", ".json"),
                ImmutablePair.of("file4", ".jpg"),
                ImmutablePair.of("file5", ".JSON"),
                ImmutablePair.of("file6", ".PARQUET")
        );

        List<String> expectedObjectKeys = new ArrayList<>();
        expectedObjectKeys.add("file1.txt");
        expectedObjectKeys.add("file2.parquet");
        expectedObjectKeys.add("file3.json");
        expectedObjectKeys.add("file4.jpg");
        expectedObjectKeys.add("file5.JSON");
        expectedObjectKeys.add("file6.PARQUET");

        Instant lastModifiedDate = Instant.now(fixedClock).minusNanos(1);
        givenObjectListingSucceeds(createObjects(objectKeys, lastModifiedDate));

        List<String> returnedObjectKeys = underTest.getObjectsOlderThan(SOURCE_BUCKET, TEST_FOLDER, matchAllFiles, zeroDayPeriod, fixedClock)
                .stream()
                .map(x -> x.key)
                .toList();

        assertThat(listObjectsV2RequestCaptor.getValue().bucket(), is(equalTo(SOURCE_BUCKET)));
        assertThat(returnedObjectKeys, containsInAnyOrder(expectedObjectKeys.toArray()));
    }

    @Test
    void getObjectsOlderThanShouldReturnListOfObjectsOlderThanSpecifiedPeriod() {
        ImmutableSet<ImmutablePair<String, String>> recentObjectKeys = ImmutableSet.of(
                ImmutablePair.of("file1", ".parquet"),
                ImmutablePair.of("file2", ".parquet"),
                ImmutablePair.of("file3", ".parquet")
        );

        ImmutableSet<ImmutablePair<String, String>> oldObjectKeys = ImmutableSet.of(
                ImmutablePair.of("file4", ".parquet"),
                ImmutablePair.of("file5", ".parquet")
        );

        List<String> expectedObjectKeys = new ArrayList<>();
        expectedObjectKeys.add("file4.parquet");
        expectedObjectKeys.add("file5.parquet");

        Instant recentLastModifiedDate = Instant.now(fixedClock).plus(1, ChronoUnit.MILLIS);

        Instant oldLastModifiedDate = Instant.now(fixedClock).minus(1, ChronoUnit.MILLIS);

        List<S3Object> recentObjects = createObjects(recentObjectKeys, recentLastModifiedDate);
        List<S3Object> oldObjects = createObjects(oldObjectKeys, oldLastModifiedDate);
        List<S3Object> allObjects = Stream.concat(recentObjects.stream(), oldObjects.stream()).toList();

        givenObjectListingSucceeds(allObjects);

        List<String> returnedObjectKeys = underTest.getObjectsOlderThan(SOURCE_BUCKET, TEST_FOLDER, jsonOrParquetFileRegex, zeroDayPeriod, fixedClock)
                .stream()
                .map(x -> x.key)
                .toList();

        assertThat(listObjectsV2RequestCaptor.getValue().bucket(), is(equalTo(SOURCE_BUCKET)));
        assertThat(returnedObjectKeys, containsInAnyOrder(expectedObjectKeys.toArray()));
    }

    @NotNull
    private static List<S3Object> createObjects(
            ImmutableSet<ImmutablePair<String, String>> objectKeys,
            Instant lastModifiedDate
    ) {
        return objectKeys.stream()
                .map(objectKey -> S3Object.builder()
                        .key(objectKey.getLeft() + objectKey.getRight())
                        .lastModified(lastModifiedDate)
                        .build()).toList();
    }

    private void givenObjectListingSucceeds(List<S3Object> objects) {
        when(listObjectsV2Response.contents()).thenReturn(objects);
        when(listObjectsV2Response.nextContinuationToken()).thenReturn("");
        when(listObjectsV2Response.isTruncated()).thenReturn(false);
        when(mockS3Client.listObjectsV2(listObjectsV2RequestCaptor.capture())).thenReturn(listObjectsV2Response);
    }

    @SuppressWarnings({"unchecked", "varargs"})
    private void givenMultiPageObjectListingSucceeds(List<S3Object> firstPage, List<S3Object> secondPage) {
        when(listObjectsV2Response.contents()).thenReturn(firstPage, secondPage);
        when(listObjectsV2Response.isTruncated()).thenReturn(true, false);
        when(mockS3Client.listObjectsV2(listObjectsV2RequestCaptor.capture())).thenReturn(listObjectsV2Response);
    }
}
