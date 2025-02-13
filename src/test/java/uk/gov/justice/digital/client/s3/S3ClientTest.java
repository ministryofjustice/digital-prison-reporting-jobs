package uk.gov.justice.digital.client.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableSet;
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

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.any;
import static uk.gov.justice.digital.test.Fixtures.fixedClock;
import static uk.gov.justice.digital.test.Fixtures.fixedDateTime;
import static uk.gov.justice.digital.common.RegexPatterns.matchAllFiles;
import static uk.gov.justice.digital.common.RegexPatterns.parquetFileRegex;
import static uk.gov.justice.digital.common.RegexPatterns.jsonOrParquetFileRegex;

@ExtendWith(MockitoExtension.class)
class S3ClientTest {

    @Mock
    S3ClientProvider mockS3ClientProvider;
    @Mock
    AmazonS3 mockS3Client;
    @Mock
    ObjectListing mockObjectListing;
    @Mock
    JobArguments mockJobArgs;
    @Mock
    DeleteObjectsResult mockDeleteObjectsResult;
    @Captor
    ArgumentCaptor<ListObjectsRequest> listObjectsRequestCaptor;
    @Captor
    ArgumentCaptor<DeleteObjectsRequest> deleteObjectsRequestCaptor;

    private static final String SOURCE_KEY = "test-source-key";
    private static final String DESTINATION_KEY = "test-destination-key";
    private static final String SOURCE_BUCKET = "test-source-bucket";
    private static final String DESTINATION_BUCKET = "test-destination-bucket";
    private static final String TEST_FOLDER = "test-folder";
    private static final Integer MAX_OBJECTS_PER_PAGE = 10;
    private static final Duration zeroDayPeriod = Duration.of(0L, ChronoUnit.DAYS);
    private static final Duration oneHourPeriod = Duration.of(1L, ChronoUnit.HOURS);

    private S3ObjectClient underTest;

    @BeforeEach
    public void setUp() {
        reset(mockS3ClientProvider, mockS3Client, mockObjectListing, mockJobArgs, mockDeleteObjectsResult);

        when(mockS3ClientProvider.getClient()).thenReturn(mockS3Client);
        when(mockJobArgs.getMaxObjectsPerPage()).thenReturn(MAX_OBJECTS_PER_PAGE);
        underTest = new S3ObjectClient(mockS3ClientProvider, mockJobArgs);
    }

    @Test
    void copyObjectShouldDeleteObjects() {
        underTest.copyObject(SOURCE_KEY, DESTINATION_KEY, SOURCE_BUCKET, DESTINATION_BUCKET);

        verify(mockS3Client).copyObject(SOURCE_BUCKET, SOURCE_KEY, DESTINATION_BUCKET, DESTINATION_KEY);
    }

    @Test
    void copyObjectShouldFailWhenClientThrowsAnException() {
        doThrow(new RuntimeException("client exception")).when(mockS3Client).copyObject(any(), any(), any(), any());

        assertThrows(RuntimeException.class, () -> underTest.copyObject(SOURCE_KEY, DESTINATION_KEY, SOURCE_BUCKET, DESTINATION_BUCKET));
    }

    @Test
    void deleteObjectsShouldDeleteObjectsReturningEmptySetWhenNoObjectsFailed() {
        when(mockDeleteObjectsResult.getDeletedObjects()).thenReturn(Collections.emptyList());
        when(mockS3Client.deleteObjects(deleteObjectsRequestCaptor.capture())).thenReturn(mockDeleteObjectsResult);

        Set<String> failedObjects = underTest.deleteObjects(Collections.singletonList(SOURCE_KEY), SOURCE_BUCKET);

        DeleteObjectsRequest deleteObjectsRequest = deleteObjectsRequestCaptor.getValue();
        List<String> keysToDelete = deleteObjectsRequest
                .getKeys()
                .stream()
                .map(DeleteObjectsRequest.KeyVersion::getKey)
                .collect(Collectors.toList());

        assertThat(failedObjects, is(empty()));
        assertThat(keysToDelete, containsInAnyOrder(SOURCE_KEY));
        assertTrue(deleteObjectsRequest.getQuiet());
    }

    @Test
    void deleteObjectsShouldDeleteObjectsReturningSetOfFailedKeys() {
        List<String> objectsKeysToDelete = new ArrayList<>();
        objectsKeysToDelete.add("key1");
        objectsKeysToDelete.add("key2");
        objectsKeysToDelete.add("key3");

        DeleteObjectsResult.DeletedObject deletedObject1 = new DeleteObjectsResult.DeletedObject();
        DeleteObjectsResult.DeletedObject deletedObject2 = new DeleteObjectsResult.DeletedObject();

        List<DeleteObjectsResult.DeletedObject> deletedObjects = new ArrayList<>();
        deletedObject1.setKey("key1");
        deletedObject2.setKey("key2");

        deletedObjects.add(deletedObject1);
        deletedObjects.add(deletedObject2);


        when(mockDeleteObjectsResult.getDeletedObjects()).thenReturn(deletedObjects);
        when(mockS3Client.deleteObjects(any())).thenReturn(mockDeleteObjectsResult);

        Set<String> failedObjects = underTest.deleteObjects(objectsKeysToDelete, SOURCE_BUCKET);

        assertThat(failedObjects, containsInAnyOrder("key1", "key2"));
    }

    @Test
    void deleteObjectsShouldFailWhenClientThrowsAnException() {
        List<String> keysToDelete = Collections.singletonList(SOURCE_KEY);

        doThrow(new RuntimeException("client exception")).when(mockS3Client).deleteObjects(any());

        assertThrows(RuntimeException.class, () -> underTest.deleteObjects(keysToDelete, SOURCE_BUCKET));
    }

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

        Date lastModifiedDate = new Date();
        lastModifiedDate.setTime(fixedDateTime.minusNanos(1).toInstant(ZoneOffset.UTC).toEpochMilli());
        givenObjectListingSucceeds(createObjectSummaries(objectKeys, lastModifiedDate));

        List<String> returnedObjectKeys = underTest.getObjectsOlderThan(SOURCE_BUCKET, TEST_FOLDER, jsonOrParquetFileRegex, zeroDayPeriod, fixedClock)
                .stream()
                .map(x -> x.key)
                .collect(Collectors.toList());

        ListObjectsRequest listObjectsRequest = listObjectsRequestCaptor.getValue();
        assertThat(listObjectsRequest.getBucketName(), is(equalTo(SOURCE_BUCKET)));
        assertThat(listObjectsRequest.getMaxKeys(), is(equalTo(MAX_OBJECTS_PER_PAGE)));
        assertThat(listObjectsRequest.getPrefix(), is(equalTo(TEST_FOLDER)));
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

        Date lastModifiedDate = new Date();
        lastModifiedDate.setTime(fixedDateTime.minusNanos(1).toInstant(ZoneOffset.UTC).toEpochMilli());
        List<S3ObjectSummary> firstPageSummaries = createObjectSummaries(firstSetOfObjectKeys, lastModifiedDate);
        List<S3ObjectSummary> secondPageSummaries = createObjectSummaries(secondSetOfObjectKeys, lastModifiedDate);

        givenMultiPageObjectListingSucceeds(firstPageSummaries, secondPageSummaries);

        List<String> returnedObjectKeys = underTest.getObjectsOlderThan(SOURCE_BUCKET, TEST_FOLDER, parquetFileRegex, zeroDayPeriod, fixedClock)
                .stream()
                .map(x -> x.key)
                .collect(Collectors.toList());

        ListObjectsRequest listObjectsRequest = listObjectsRequestCaptor.getValue();
        assertThat(listObjectsRequest.getBucketName(), is(equalTo(SOURCE_BUCKET)));
        assertThat(listObjectsRequest.getMaxKeys(), is(equalTo(MAX_OBJECTS_PER_PAGE)));
        assertThat(listObjectsRequest.getPrefix(), is(equalTo(TEST_FOLDER)));
        assertThat(returnedObjectKeys, containsInAnyOrder(expectedObjectKeys.toArray()));
    }

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

        Date lastModifiedDate = new Date();
        lastModifiedDate.setTime(fixedDateTime.minusNanos(1).toInstant(ZoneOffset.UTC).toEpochMilli());
        givenObjectListingSucceeds(createObjectSummaries(objectKeys, lastModifiedDate));

        List<String> returnedObjectKeys = underTest.getObjectsOlderThan(SOURCE_BUCKET, TEST_FOLDER, matchAllFiles, zeroDayPeriod, fixedClock)
                .stream()
                .map(x -> x.key)
                .collect(Collectors.toList());

        assertThat(listObjectsRequestCaptor.getValue().getBucketName(), is(equalTo(SOURCE_BUCKET)));
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

        Date recentLastModifiedDate = new Date();
        recentLastModifiedDate.setTime(fixedDateTime.plusNanos(1).toInstant(ZoneOffset.UTC).toEpochMilli());

        Date oldLastModifiedDate = new Date();
        oldLastModifiedDate.setTime(fixedDateTime.minusNanos(1).toInstant(ZoneOffset.UTC).toEpochMilli());

        List<S3ObjectSummary> recentObjectSummaries = createObjectSummaries(recentObjectKeys, recentLastModifiedDate);
        List<S3ObjectSummary> oldObjectSummaries = createObjectSummaries(oldObjectKeys, oldLastModifiedDate);
        List<S3ObjectSummary> allObjectSummaries = Stream.concat(recentObjectSummaries.stream(), oldObjectSummaries.stream()).collect(Collectors.toList());

        givenObjectListingSucceeds(allObjectSummaries);

        List<String> returnedObjectKeys = underTest.getObjectsOlderThan(SOURCE_BUCKET, TEST_FOLDER, jsonOrParquetFileRegex, zeroDayPeriod, fixedClock)
                .stream()
                .map(x -> x.key)
                .collect(Collectors.toList());

        assertThat(listObjectsRequestCaptor.getValue().getBucketName(), is(equalTo(SOURCE_BUCKET)));
        assertThat(returnedObjectKeys, containsInAnyOrder(expectedObjectKeys.toArray()));
    }

    @Test
    void getObjectsNewerThanShouldReturnListOfObjectsMatchingAllowedExtensionsWithinGivenFolderPrefix() {
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

        Date lastModifiedDate = new Date();
        lastModifiedDate.setTime(fixedDateTime.plusNanos(1).toInstant(ZoneOffset.UTC).toEpochMilli());
        givenObjectListingSucceeds(createObjectSummaries(objectKeys, lastModifiedDate));

        List<String> returnedObjectKeys = underTest.getObjectsNewerThan(SOURCE_BUCKET, TEST_FOLDER, jsonOrParquetFileRegex, zeroDayPeriod, fixedClock)
                .stream()
                .map(x -> x.key)
                .collect(Collectors.toList());

        ListObjectsRequest listObjectsRequest = listObjectsRequestCaptor.getValue();
        assertThat(listObjectsRequest.getBucketName(), is(equalTo(SOURCE_BUCKET)));
        assertThat(listObjectsRequest.getMaxKeys(), is(equalTo(MAX_OBJECTS_PER_PAGE)));
        assertThat(listObjectsRequest.getPrefix(), is(equalTo(TEST_FOLDER)));
        assertThat(returnedObjectKeys, containsInAnyOrder(expectedObjectKeys.toArray()));
    }

    @Test
    void getObjectsNewerThanShouldReturnListOfObjectsMatchingAllowedExtensionsWhenObjectsListExceedsOnePage() {
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

        Date lastModifiedDate = new Date();
        lastModifiedDate.setTime(fixedDateTime.plusNanos(1).toInstant(ZoneOffset.UTC).toEpochMilli());
        List<S3ObjectSummary> firstPageSummaries = createObjectSummaries(firstSetOfObjectKeys, lastModifiedDate);
        List<S3ObjectSummary> secondPageSummaries = createObjectSummaries(secondSetOfObjectKeys, lastModifiedDate);

        givenMultiPageObjectListingSucceeds(firstPageSummaries, secondPageSummaries);

        List<String> returnedObjectKeys = underTest.getObjectsNewerThan(SOURCE_BUCKET, TEST_FOLDER, parquetFileRegex, zeroDayPeriod, fixedClock)
                .stream()
                .map(x -> x.key)
                .collect(Collectors.toList());

        ListObjectsRequest listObjectsRequest = listObjectsRequestCaptor.getValue();
        assertThat(listObjectsRequest.getBucketName(), is(equalTo(SOURCE_BUCKET)));
        assertThat(listObjectsRequest.getMaxKeys(), is(equalTo(MAX_OBJECTS_PER_PAGE)));
        assertThat(listObjectsRequest.getPrefix(), is(equalTo(TEST_FOLDER)));
        assertThat(returnedObjectKeys, containsInAnyOrder(expectedObjectKeys.toArray()));
    }

    @Test
    void getObjectsNewerThanShouldReturnListOfAllObjectsWhenGivenWildCardExtension() {
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

        Date lastModifiedDate = new Date();
        lastModifiedDate.setTime(fixedDateTime.plusNanos(1).toInstant(ZoneOffset.UTC).toEpochMilli());
        givenObjectListingSucceeds(createObjectSummaries(objectKeys, lastModifiedDate));

        List<String> returnedObjectKeys = underTest.getObjectsNewerThan(SOURCE_BUCKET, TEST_FOLDER, matchAllFiles, zeroDayPeriod, fixedClock)
                .stream()
                .map(x -> x.key)
                .collect(Collectors.toList());

        assertThat(listObjectsRequestCaptor.getValue().getBucketName(), is(equalTo(SOURCE_BUCKET)));
        assertThat(returnedObjectKeys, containsInAnyOrder(expectedObjectKeys.toArray()));
    }

    @Test
    void getObjectsNewerThanShouldReturnListOfObjectsNewerThanSpecifiedPeriod() {
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
        expectedObjectKeys.add("file1.parquet");
        expectedObjectKeys.add("file2.parquet");
        expectedObjectKeys.add("file3.parquet");

        Date recentLastModifiedDate = new Date();
        recentLastModifiedDate.setTime(fixedDateTime.minusHours(1).toInstant(ZoneOffset.UTC).toEpochMilli());

        Date oldLastModifiedDate = new Date();
        oldLastModifiedDate.setTime(fixedDateTime.minusHours(1).minusNanos(1).toInstant(ZoneOffset.UTC).toEpochMilli());

        List<S3ObjectSummary> recentObjectSummaries = createObjectSummaries(recentObjectKeys, recentLastModifiedDate);
        List<S3ObjectSummary> oldObjectSummaries = createObjectSummaries(oldObjectKeys, oldLastModifiedDate);
        List<S3ObjectSummary> allObjectSummaries = Stream.concat(recentObjectSummaries.stream(), oldObjectSummaries.stream()).collect(Collectors.toList());

        when(mockObjectListing.getObjectSummaries()).thenReturn(allObjectSummaries);
        when(mockS3Client.listObjects(listObjectsRequestCaptor.capture())).thenReturn(mockObjectListing);

        List<String> returnedObjectKeys = underTest.getObjectsNewerThan(SOURCE_BUCKET, TEST_FOLDER, jsonOrParquetFileRegex, oneHourPeriod, fixedClock)
                .stream()
                .map(x -> x.key)
                .collect(Collectors.toList());

        assertThat(listObjectsRequestCaptor.getValue().getBucketName(), is(equalTo(SOURCE_BUCKET)));
        assertThat(returnedObjectKeys, containsInAnyOrder(expectedObjectKeys.toArray()));
    }

    @NotNull
    private static List<S3ObjectSummary> createObjectSummaries(
            ImmutableSet<ImmutablePair<String, String>> objectKeys,
            Date lastModifiedDate
    ) {
        return objectKeys.stream()
                .map(objectKey -> {
                    S3ObjectSummary objectSummary = new S3ObjectSummary();
                    String extension = objectKey.getRight();
                    objectSummary.setKey(objectKey.getLeft() + extension);
                    objectSummary.setLastModified(lastModifiedDate);
                    return objectSummary;
                }).collect(Collectors.toList());
    }

    private void givenObjectListingSucceeds(List<S3ObjectSummary> objectSummaries) {
        when(mockObjectListing.getObjectSummaries()).thenReturn(objectSummaries);
        when(mockObjectListing.isTruncated()).thenReturn(false);
        when(mockS3Client.listObjects(listObjectsRequestCaptor.capture())).thenReturn(mockObjectListing);
    }

    @SuppressWarnings({"unchecked", "varargs"})
    private void givenMultiPageObjectListingSucceeds(List<S3ObjectSummary> firstPage, List<S3ObjectSummary> secondPage) {
        when(mockObjectListing.getObjectSummaries()).thenReturn(firstPage, secondPage);
        when(mockObjectListing.isTruncated()).thenReturn(true, false);
        when(mockS3Client.listObjects(listObjectsRequestCaptor.capture())).thenReturn(mockObjectListing);
    }
}
