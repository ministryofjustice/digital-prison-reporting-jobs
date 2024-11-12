package uk.gov.justice.digital.service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3ObjectClient;
import uk.gov.justice.digital.config.JobArguments;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doNothing;
import static uk.gov.justice.digital.client.s3.S3ObjectClient.DELIMITER;
import static uk.gov.justice.digital.test.Fixtures.fixedClock;
import static uk.gov.justice.digital.test.TestHelpers.givenConfiguredRetriesJobArgs;

@ExtendWith(MockitoExtension.class)
class S3FileServiceTest {

    private static final String SOURCE_BUCKET = "source-bucket";
    private static final String SOURCE_PREFIX = "source-prefix";
    private static final String DESTINATION_BUCKET = "destination-bucket";
    private static final String DESTINATION_PREFIX = "destination-prefix";
    private static final long RETENTION_AMOUNT = 2L;
    private static final Duration retentionPeriod = Duration.of(RETENTION_AMOUNT, ChronoUnit.DAYS);

    private static final ImmutableSet<String> parquetFileExtension = ImmutableSet.of(".parquet");

    @Mock
    private S3ObjectClient mockS3Client;
    @Mock
    private JobArguments mockJobArguments;

    private S3FileService undertest;

    @BeforeEach
    public void setup() {
        reset(mockS3Client, mockJobArguments);
        givenConfiguredRetriesJobArgs(1, mockJobArguments);
        undertest = new S3FileService(mockS3Client, fixedClock, mockJobArguments);
    }

    @Test
    void listFilesShouldReturnEmptyListWhenThereAreNoParquetFiles() {
        when(mockS3Client.getObjectsOlderThan(any(), any(), any(), any(), any())).thenReturn(Collections.emptyList());

        List<String> result = undertest.listFiles(SOURCE_BUCKET, SOURCE_PREFIX, parquetFileExtension, retentionPeriod);

        assertThat(result, is(empty()));
    }

    @Test
    void listFilesShouldReturnListOfParquetFiles() {
        List<String> expected = new ArrayList<>();
        expected.add("file1.parquet");
        expected.add("file2.parquet");
        expected.add("file3.parquet");
        expected.add("file4.parquet");

        when(mockS3Client.getObjectsOlderThan(SOURCE_BUCKET, SOURCE_PREFIX, parquetFileExtension, retentionPeriod, fixedClock))
                .thenReturn(expected);

        List<String> result = undertest.listFiles(SOURCE_BUCKET, SOURCE_PREFIX, parquetFileExtension, retentionPeriod);

        assertThat(result, containsInAnyOrder(expected.toArray()));
    }

    @Test
    void listFilesShouldRetryWhenErrorOccursDuringListingOfFiles() {
        int numRetries = 2;
        givenConfiguredRetriesJobArgs(numRetries, mockJobArguments);
        doThrow(new AmazonS3Exception("s3 error")).when(mockS3Client).getObjectsOlderThan(any(), any(), any(), any(), any());

        S3FileService s3FileService = new S3FileService(mockS3Client, fixedClock, mockJobArguments);

        assertThrows(AmazonS3Exception.class, () -> s3FileService.listFiles(SOURCE_BUCKET, SOURCE_PREFIX, parquetFileExtension, retentionPeriod));

        verify(mockS3Client, times(numRetries)).getObjectsOlderThan(any(), any(), any(), any(), any());
    }

    @Test
    void listFilesForConfigShouldReturnEmptyListWhenThereAreNoParquetFilesForConfiguredTables() {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema_1", "table_1"),
                ImmutablePair.of("schema_2", "table_2")
        );

        when(mockS3Client.getObjectsOlderThan(any(), any(), any(), any(), any())).thenReturn(Collections.emptyList());

        List<String> result = undertest.listFilesForConfig(SOURCE_BUCKET, SOURCE_PREFIX, configuredTables, parquetFileExtension, retentionPeriod);

        assertThat(result, is(empty()));
    }

    @Test
    void listFilesForConfigShouldListFilesInFolderPrefix() {
        ImmutablePair<String, String> configuredTable = ImmutablePair.of("schema_1", "table_1");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(configuredTable);

        undertest.listFilesForConfig(SOURCE_BUCKET, SOURCE_PREFIX, configuredTables, parquetFileExtension, retentionPeriod);

        String folder = SOURCE_PREFIX + DELIMITER + configuredTable.left + DELIMITER + configuredTable.right + DELIMITER;
        verify(mockS3Client, times(1))
                .getObjectsOlderThan(eq(SOURCE_BUCKET), eq(folder), any(), eq(retentionPeriod), any());
    }

    @Test
    void listFilesForConfigShouldListFilesWhenNoFolderPrefixIsGiven() {
        ImmutablePair<String, String> configuredTable = ImmutablePair.of("schema_1", "table_1");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(configuredTable);

        undertest.listFilesForConfig(SOURCE_BUCKET, "", configuredTables, parquetFileExtension, retentionPeriod);

        String folder = configuredTable.left + DELIMITER + configuredTable.right + DELIMITER;
        verify(mockS3Client, times(1))
                .getObjectsOlderThan(eq(SOURCE_BUCKET), eq(folder), any(), eq(retentionPeriod), any());
    }

    @Test
    void listFilesForConfigShouldReturnListOfParquetFilesRelatedToConfiguredTables() {
        String configuredTable1 = "schema_1/table_1";
        String configuredTable2 = "schema_2/table_2";

        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema_1", "table_1"),
                ImmutablePair.of("schema_2", "table_2")
        );

        List<String> expectedFilesForTable1 = new ArrayList<>();
        expectedFilesForTable1.add("file1.parquet");
        expectedFilesForTable1.add("file2.parquet");
        expectedFilesForTable1.add("file3.parquet");

        List<String> expectedFilesForTable2 = new ArrayList<>();
        expectedFilesForTable2.add("file4.parquet");
        expectedFilesForTable2.add("file5.parquet");

        when(mockS3Client.getObjectsOlderThan(
                SOURCE_BUCKET,
                SOURCE_PREFIX + DELIMITER + configuredTable1 + DELIMITER,
                parquetFileExtension,
                retentionPeriod,
                fixedClock)).thenReturn(expectedFilesForTable1);

        when(mockS3Client.getObjectsOlderThan(
                SOURCE_BUCKET,
                SOURCE_PREFIX + DELIMITER + configuredTable2 + DELIMITER,
                parquetFileExtension,
                retentionPeriod,
                fixedClock)).thenReturn(expectedFilesForTable2);

        List<String> result = undertest.listFilesForConfig(SOURCE_BUCKET, SOURCE_PREFIX, configuredTables, parquetFileExtension, retentionPeriod);

        List<String> expectedResult = new ArrayList<>();
        expectedResult.addAll(expectedFilesForTable1);
        expectedResult.addAll(expectedFilesForTable2);

        assertThat(result, containsInAnyOrder(expectedResult.toArray()));
    }

    @Test
    void listFilesForConfigShouldRetryWhenErrorOccursDuringListingOfFiles() {
        int numRetries = 2;
        givenConfiguredRetriesJobArgs(numRetries, mockJobArguments);
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(ImmutablePair.of("schema_1", "table_1"));
        doThrow(new AmazonS3Exception("s3 error")).when(mockS3Client).getObjectsOlderThan(any(), any(), any(), any(), any());

        S3FileService s3FileService = new S3FileService(mockS3Client, fixedClock, mockJobArguments);

        assertThrows(AmazonS3Exception.class, () -> s3FileService.listFilesForConfig(SOURCE_BUCKET, SOURCE_PREFIX, configuredTables, parquetFileExtension, retentionPeriod));

        verify(mockS3Client, times(numRetries)).getObjectsOlderThan(any(), any(), any(), any(), any());
    }

    @Test
    void copyObjectsShouldCopyGivenObjectsFromSourceToDestinationBucketsWhenDeleteCopiedFilesIsFalse() {
        List<String> objectKeys = new ArrayList<>();
        objectKeys.add("file1.parquet");
        objectKeys.add("file2.parquet");
        objectKeys.add("file3.parquet");
        objectKeys.add("file4.parquet");

        Set<String> failedObjects = undertest.copyObjects(objectKeys, SOURCE_BUCKET, SOURCE_PREFIX, DESTINATION_BUCKET, DESTINATION_PREFIX, false);

        verify(mockS3Client, times(objectKeys.size())).copyObject(any(), any(), eq(SOURCE_BUCKET), eq(DESTINATION_BUCKET));

        assertThat(failedObjects, is(empty()));
    }

    @Test
    void copyObjectsShouldRemoveSourcePrefixWhenDestinationPrefixIsEmpty() {
        List<String> objectKeys = new ArrayList<>();
        String objectKey = SOURCE_PREFIX + "/file1.parquet";
        objectKeys.add(objectKey);

        undertest.copyObjects(objectKeys, SOURCE_BUCKET, SOURCE_PREFIX, DESTINATION_BUCKET, "", false);

        verify(mockS3Client, times(objectKeys.size()))
                .copyObject(objectKey, "file1.parquet", SOURCE_BUCKET, DESTINATION_BUCKET);
    }

    @Test
    void copyObjectsAndUseDestinationPrefixWhenSourcePrefixIsEmpty() {
        List<String> objectKeys = new ArrayList<>();
        String objectKey = "file1.parquet";
        objectKeys.add(objectKey);

        undertest.copyObjects(objectKeys, SOURCE_BUCKET, "", DESTINATION_BUCKET, DESTINATION_PREFIX, false);

        verify(mockS3Client, times(objectKeys.size()))
                .copyObject(objectKey, DESTINATION_PREFIX + DELIMITER + objectKey, SOURCE_BUCKET, DESTINATION_BUCKET);
    }

    @Test
    void copyObjectsWhenBothDestinationPrefixAndSourcePrefixAreEmpty() {
        List<String> objectKeys = new ArrayList<>();
        String objectKey = "file1.parquet";
        objectKeys.add(objectKey);

        undertest.copyObjects(objectKeys, SOURCE_BUCKET, "", DESTINATION_BUCKET, "", false);

        verify(mockS3Client, times(objectKeys.size()))
                .copyObject(objectKey, objectKey, SOURCE_BUCKET, DESTINATION_BUCKET);
    }

    @Test
    void copyObjectsShouldReplaceSourcePrefixWithDestinationPrefixWhenDestinationPrefixIsNonEmpty() {
        List<String> objectKeys = new ArrayList<>();
        String objectKey = SOURCE_PREFIX + "/file1.parquet";
        objectKeys.add(objectKey);

        undertest.copyObjects(objectKeys, SOURCE_BUCKET, SOURCE_PREFIX, DESTINATION_BUCKET, DESTINATION_PREFIX, false);

        verify(mockS3Client, times(objectKeys.size()))
                .copyObject(objectKey, DESTINATION_PREFIX + "/file1.parquet", SOURCE_BUCKET, DESTINATION_BUCKET);
    }

    @Test
    void copyObjectsShouldReturnListOfFailedObjectsWhenDeleteCopiedFilesIsFalse() {
        List<String> objectKeys = new ArrayList<>();
        objectKeys.add("file1.parquet");
        objectKeys.add("file2.parquet");
        objectKeys.add("file3.parquet");
        objectKeys.add("file4.parquet");

        Set<String> expectedFailedObjects = new HashSet<>();
        expectedFailedObjects.add("file1.parquet");
        expectedFailedObjects.add("file2.parquet");
        expectedFailedObjects.add("file4.parquet");

        doThrow(new AmazonServiceException("failure")).when(mockS3Client).copyObject(any(), any(), any(), any());
        doNothing().when(mockS3Client).copyObject(eq("file3.parquet"), any(), any(), any());

        Set<String> failedObjects = undertest.copyObjects(objectKeys, SOURCE_BUCKET, SOURCE_PREFIX, DESTINATION_BUCKET, DESTINATION_PREFIX, false);

        assertEquals(failedObjects, expectedFailedObjects);
    }

    @Test
    void copyObjectsShouldCopyAndDeleteGivenObjectsFromSourceToDestinationBucketsWhenDeleteCopiedFilesIsTrue() {
        List<String> objectKeys = new ArrayList<>();
        objectKeys.add("file1.parquet");
        objectKeys.add("file2.parquet");
        objectKeys.add("file3.parquet");
        objectKeys.add("file4.parquet");

        Set<String> failedObjects = undertest.copyObjects(objectKeys, SOURCE_BUCKET, SOURCE_PREFIX, DESTINATION_BUCKET, DESTINATION_PREFIX, true);

        verify(mockS3Client, times(objectKeys.size())).copyObject(any(), any(), eq(SOURCE_BUCKET), eq(DESTINATION_BUCKET));
        verify(mockS3Client, times(objectKeys.size())).deleteObject(any(), eq(SOURCE_BUCKET));

        assertThat(failedObjects, is(empty()));
    }

    @Test
    void copyObjectsShouldReturnListOfObjectsWhichFailedToBeCopiedWhenDeleteCopiedFilesIsTrue() {
        List<String> objectKeys = new ArrayList<>();
        objectKeys.add("file1.parquet");
        objectKeys.add("file2.parquet");
        objectKeys.add("file3.parquet");
        objectKeys.add("file4.parquet");

        Set<String> expectedFailedObjects = new HashSet<>();
        expectedFailedObjects.add("file1.parquet");
        expectedFailedObjects.add("file2.parquet");
        expectedFailedObjects.add("file4.parquet");

        doThrow(new AmazonServiceException("failure")).when(mockS3Client).copyObject(any(), any(), any(), any());
        doNothing().when(mockS3Client).copyObject(eq("file3.parquet"), any(), any(), any());

        Set<String> failedObjects = undertest.copyObjects(objectKeys, SOURCE_BUCKET, SOURCE_PREFIX, DESTINATION_BUCKET, DESTINATION_PREFIX, true);

        assertEquals(failedObjects, expectedFailedObjects);
    }

    @Test
    void copyObjectsShouldReturnListOfObjectsWhichFailedToBeDeletedWhenDeleteCopiedFilesIsTrue() {
        List<String> objectKeys = new ArrayList<>();
        objectKeys.add("file1.parquet");
        objectKeys.add("file2.parquet");
        objectKeys.add("file3.parquet");
        objectKeys.add("file4.parquet");

        Set<String> expectedFailedObjects = new HashSet<>();
        expectedFailedObjects.add("file1.parquet");
        expectedFailedObjects.add("file2.parquet");
        expectedFailedObjects.add("file4.parquet");

        doNothing().when(mockS3Client).copyObject(any(), any(), any(), any());
        doThrow(new AmazonServiceException("failure")).when(mockS3Client).deleteObject(any(), any());
        doNothing().when(mockS3Client).deleteObject(eq("file3.parquet"), any());

        Set<String> failedObjects = undertest.copyObjects(objectKeys, SOURCE_BUCKET, SOURCE_PREFIX, DESTINATION_BUCKET, DESTINATION_PREFIX, true);

        assertEquals(failedObjects, expectedFailedObjects);
    }

    @Test
    void copyObjectsShouldRetryWhenAnErrorOccursCopyingFileAndThereIsARetryConfig() {
        int numRetries = 2;
        List<String> objectKeys = Collections.singletonList("file1.parquet");
        givenConfiguredRetriesJobArgs(numRetries, mockJobArguments);
        doThrow(new AmazonS3Exception("s3 error")).when(mockS3Client).copyObject(any(), any(), any(), any());

        S3FileService s3FileService = new S3FileService(mockS3Client, fixedClock, mockJobArguments);

        s3FileService.copyObjects(objectKeys, SOURCE_BUCKET, SOURCE_PREFIX, DESTINATION_BUCKET, DESTINATION_PREFIX, false);

        verify(mockS3Client, times(numRetries)).copyObject(any(), any(), any(), any());
    }

    @Test
    void copyObjectsShouldRetryWhenAnErrorOccursDeletingFileAndThereIsARetryConfig() {
        int numRetries = 2;
        List<String> objectKeys = Collections.singletonList("file1.parquet");
        givenConfiguredRetriesJobArgs(numRetries, mockJobArguments);
        doNothing().when(mockS3Client).copyObject(any(), any(), any(), any());
        doThrow(new AmazonS3Exception("s3 error")).when(mockS3Client).deleteObject(any(), any());

        S3FileService s3FileService = new S3FileService(mockS3Client, fixedClock, mockJobArguments);

        s3FileService.copyObjects(objectKeys, SOURCE_BUCKET, SOURCE_PREFIX, DESTINATION_BUCKET, DESTINATION_PREFIX, true);

        verify(mockS3Client, times(numRetries)).deleteObject(any(), any());
    }

    @Test
    void deleteObjectsShouldDeleteGivenObjectsFromS3() {
        List<String> objectKeys = new ArrayList<>();
        objectKeys.add("file1.parquet");
        objectKeys.add("file2.parquet");
        objectKeys.add("file3.parquet");
        objectKeys.add("file4.parquet");

        Set<String> failedObjects = undertest.deleteObjects(objectKeys, SOURCE_BUCKET);

        verify(mockS3Client, times(objectKeys.size())).deleteObject(any(), eq(SOURCE_BUCKET));

        assertThat(failedObjects, is(empty()));
    }

    @Test
    void deleteObjectsShouldReturnListOfObjectsWhichFailedDeletion() {
        List<String> objectKeys = new ArrayList<>();
        objectKeys.add("file1.parquet");
        objectKeys.add("file2.parquet");
        objectKeys.add("file3.parquet");
        objectKeys.add("file4.parquet");

        doNothing().when(mockS3Client).deleteObject("file1.parquet", SOURCE_BUCKET);
        doNothing().when(mockS3Client).deleteObject("file3.parquet", SOURCE_BUCKET);

        doThrow(new AmazonServiceException("failure")).when(mockS3Client).deleteObject("file2.parquet", SOURCE_BUCKET);
        doThrow(new AmazonServiceException("failure")).when(mockS3Client).deleteObject("file4.parquet", SOURCE_BUCKET);

        Set<String> failedObjects = undertest.deleteObjects(objectKeys, SOURCE_BUCKET);

        List<String> expectedFailedObjects = new ArrayList<>();
        expectedFailedObjects.add("file2.parquet");
        expectedFailedObjects.add("file4.parquet");
        assertThat(failedObjects, containsInAnyOrder(expectedFailedObjects.toArray()));
    }

    @Test
    void deleteObjectsShouldRetryWhenAnErrorOccursDeletingFile() {
        int numRetries = 2;
        List<String> objectKeys = Collections.singletonList("file1.parquet");
        givenConfiguredRetriesJobArgs(numRetries, mockJobArguments);
        doThrow(new AmazonS3Exception("s3 error")).when(mockS3Client).deleteObject(any(), any());

        S3FileService s3FileService = new S3FileService(mockS3Client, fixedClock, mockJobArguments);

        s3FileService.deleteObjects(objectKeys, SOURCE_BUCKET);

        verify(mockS3Client, times(numRetries)).deleteObject(any(), any());
    }
}