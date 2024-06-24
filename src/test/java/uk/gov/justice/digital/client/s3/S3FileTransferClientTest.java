package uk.gov.justice.digital.client.s3;

import com.amazonaws.services.s3.AmazonS3;
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
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.test.Fixtures.fixedClock;
import static uk.gov.justice.digital.test.Fixtures.fixedDateTime;

@ExtendWith(MockitoExtension.class)
public class S3FileTransferClientTest {

    @Mock
    S3ClientProvider mockS3ClientProvider;
    @Mock
    AmazonS3 mockS3Client;
    @Mock
    ObjectListing mockObjectListing;
    @Mock
    JobArguments mockJobArgs;
    @Captor
    ArgumentCaptor<ListObjectsRequest> listObjectsRequestCaptor;

    private static final String SOURCE_KEY = "test-source-key";
    private static final String DESTINATION_KEY = "test-destination-key";
    private static final String SOURCE_BUCKET = "test-source-bucket";
    private static final String DESTINATION_BUCKET = "test-destination-bucket";
    private static final ImmutableSet<String> allowedExtensions = ImmutableSet.of(".parquet", ".json");
    private static final Integer MAX_OBJECTS_PER_PAGE = 10;
    private static final Duration zeroDayRetentionPeriod = Duration.of(0L, ChronoUnit.DAYS);

    private S3ObjectClient underTest;
    @BeforeEach
    public void setUp() {
        reset(mockS3ClientProvider, mockS3Client, mockObjectListing, mockJobArgs);

        when(mockS3ClientProvider.getClient()).thenReturn(mockS3Client);
        when(mockJobArgs.getMaxObjectsPerPage()).thenReturn(MAX_OBJECTS_PER_PAGE);
        underTest = new S3ObjectClient(mockS3ClientProvider, mockJobArgs);
    }

    @Test
    public void copyObjectShouldDeleteObjects() {
        underTest.copyObject(SOURCE_KEY, DESTINATION_KEY, SOURCE_BUCKET, DESTINATION_BUCKET);

        verify(mockS3Client, times(1)).copyObject(SOURCE_BUCKET, SOURCE_KEY, DESTINATION_BUCKET, DESTINATION_KEY);
    }

    @Test
    public void copyObjectShouldFailWhenClientThrowsAnException() {
        doThrow(new RuntimeException("client exception")).when(mockS3Client).copyObject(any(), any(), any(), any());

        assertThrows(RuntimeException.class, () -> underTest.copyObject(SOURCE_KEY, DESTINATION_KEY, SOURCE_BUCKET, DESTINATION_BUCKET));
    }

    @Test
    public void deleteObjectShouldDeleteObjects() {
        underTest.deleteObject(SOURCE_KEY, SOURCE_BUCKET);

        verify(mockS3Client, times(1)).deleteObject(SOURCE_BUCKET, SOURCE_KEY);
    }

    @Test
    public void deleteObjectShouldFailWhenClientThrowsAnException() {
        doThrow(new RuntimeException("client exception")).when(mockS3Client).deleteObject(any(), any());

        assertThrows(RuntimeException.class, () -> underTest.deleteObject(SOURCE_KEY, SOURCE_BUCKET));
    }

    @Test
    public void getObjectsOlderThanShouldReturnListOfObjectsMatchingAllowedExtensions() {
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

        List<String> returnedObjectKeys = underTest.getObjectsOlderThan(SOURCE_BUCKET, allowedExtensions, zeroDayRetentionPeriod, fixedClock);

        ListObjectsRequest listObjectsRequest = listObjectsRequestCaptor.getValue();
        assertThat(listObjectsRequest.getBucketName(), is(equalTo(SOURCE_BUCKET)));
        assertThat(listObjectsRequest.getMaxKeys(), is(equalTo(MAX_OBJECTS_PER_PAGE)));
        assertThat(returnedObjectKeys, containsInAnyOrder(expectedObjectKeys.toArray()));
    }

    @Test
    public void getObjectsOlderThanShouldReturnListOfObjectsMatchingAllowedExtensionsWhenObjectsListExceedsOnePage() {
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

        List<String> returnedObjectKeys = underTest.getObjectsOlderThan(SOURCE_BUCKET, ImmutableSet.of(".parquet"), zeroDayRetentionPeriod, fixedClock);

        ListObjectsRequest listObjectsRequest = listObjectsRequestCaptor.getValue();
        assertThat(listObjectsRequest.getBucketName(), is(equalTo(SOURCE_BUCKET)));
        assertThat(listObjectsRequest.getMaxKeys(), is(equalTo(MAX_OBJECTS_PER_PAGE)));
        assertThat(returnedObjectKeys, containsInAnyOrder(expectedObjectKeys.toArray()));
    }

    @Test
    public void getObjectsOlderThanShouldReturnListOfAllObjectsWhenGivenWildCardExtension() {
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

        List<String> returnedObjectKeys = underTest.getObjectsOlderThan(SOURCE_BUCKET, ImmutableSet.of("*"), zeroDayRetentionPeriod, fixedClock);

        assertThat(listObjectsRequestCaptor.getValue().getBucketName(), is(equalTo(SOURCE_BUCKET)));
        assertThat(returnedObjectKeys, containsInAnyOrder(expectedObjectKeys.toArray()));
    }

    @Test
    public void getObjectsOlderThanShouldReturnListOfObjectsOlderThanSpecifiedNumberOfRetentionDays() {
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

        List<String> returnedObjectKeys = underTest.getObjectsOlderThan(SOURCE_BUCKET, allowedExtensions, zeroDayRetentionPeriod, fixedClock);

        assertThat(listObjectsRequestCaptor.getValue().getBucketName(), is(equalTo(SOURCE_BUCKET)));
        assertThat(returnedObjectKeys, containsInAnyOrder(expectedObjectKeys.toArray()));
    }

    @Test
    public void getObjectsOlderThanShouldListObjectsWithinGivenFolderPrefix() {
        String folder = "test-folder";

        ImmutableSet<ImmutablePair<String, String>> objectKeys = ImmutableSet.of(
                ImmutablePair.of("file1", ".parquet"),
                ImmutablePair.of("file2", ".parquet"),
                ImmutablePair.of("file6", ".parquet")
        );

        Date lastModifiedDate = new Date();
        lastModifiedDate.setTime(fixedDateTime.minusNanos(1).toInstant(ZoneOffset.UTC).toEpochMilli());
        givenObjectListingSucceeds(createObjectSummaries(objectKeys, lastModifiedDate));

        underTest.getObjectsOlderThan(SOURCE_BUCKET, folder, allowedExtensions, zeroDayRetentionPeriod, fixedClock);

        assertThat(listObjectsRequestCaptor.getValue().getBucketName(), is(equalTo(SOURCE_BUCKET)));
        assertThat(listObjectsRequestCaptor.getValue().getPrefix(), is(equalTo(folder)));
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
