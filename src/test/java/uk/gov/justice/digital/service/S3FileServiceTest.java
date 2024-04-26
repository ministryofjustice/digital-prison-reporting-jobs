package uk.gov.justice.digital.service;

import com.amazonaws.AmazonServiceException;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3FileTransferClient;

import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.client.s3.S3FileTransferClient.DELIMITER;
import static uk.gov.justice.digital.test.Fixtures.fixedClock;

@ExtendWith(MockitoExtension.class)
class S3FileServiceTest {

    private static final String SOURCE_BUCKET = "source-bucket";
    private static final String SOURCE_PREFIX = "source-prefix";
    private static final String DESTINATION_BUCKET = "destination-bucket";
    private static final String DESTINATION_PREFIX = "destination-prefix";
    private static final long RETENTION_DAYS = 2L;

    private static final ImmutableSet<String> parquetFileExtension = ImmutableSet.of(".parquet");

    @Mock
    private S3FileTransferClient mockS3Client;

    private S3FileService undertest;

    @BeforeEach
    public void setup() {
        reset(mockS3Client);

        undertest = new S3FileService(mockS3Client, fixedClock);
    }

    @Test
    public void listFilesShouldReturnEmptyListWhenThereAreNoParquetFiles() {
        when(mockS3Client.getObjectsOlderThan(any(), any(), any(), any(), any())).thenReturn(Collections.emptyList());

        List<String> result = undertest.listFiles(SOURCE_BUCKET, SOURCE_PREFIX, parquetFileExtension, RETENTION_DAYS);

        assertThat(result, is(empty()));
    }

    @Test
    public void listFilesShouldReturnListOfParquetFiles() {
        List<String> expected = new ArrayList<>();
        expected.add("file1.parquet");
        expected.add("file2.parquet");
        expected.add("file3.parquet");
        expected.add("file4.parquet");

        when(mockS3Client.getObjectsOlderThan(SOURCE_BUCKET, SOURCE_PREFIX, parquetFileExtension, RETENTION_DAYS, fixedClock))
                .thenReturn(expected);

        List<String> result = undertest.listFiles(SOURCE_BUCKET, SOURCE_PREFIX, parquetFileExtension, RETENTION_DAYS);

        assertThat(result, containsInAnyOrder(expected.toArray()));
    }

    @Test
    public void listFilesForConfigShouldReturnEmptyListWhenThereAreNoParquetFilesForConfiguredTables() {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema_1", "table_1"),
                ImmutablePair.of("schema_2", "table_2")
        );

        when(mockS3Client.getObjectsOlderThan(any(), any(), any(), any(), any())).thenReturn(Collections.emptyList());

        List<String> result = undertest.listFilesForConfig(SOURCE_BUCKET, SOURCE_PREFIX, configuredTables, parquetFileExtension, RETENTION_DAYS);

        assertThat(result, is(empty()));
    }

    @Test
    public void listFilesForConfigShouldReturnListOfParquetFilesRelatedToConfiguredTables() {
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
                RETENTION_DAYS,
                fixedClock)).thenReturn(expectedFilesForTable1);

        when(mockS3Client.getObjectsOlderThan(
                SOURCE_BUCKET,
                SOURCE_PREFIX + DELIMITER + configuredTable2 + DELIMITER,
                parquetFileExtension,
                RETENTION_DAYS,
                fixedClock)).thenReturn(expectedFilesForTable2);

        List<String> result = undertest.listFilesForConfig(SOURCE_BUCKET, SOURCE_PREFIX, configuredTables, parquetFileExtension, RETENTION_DAYS);

        List<String> expectedResult = new ArrayList<>();
        expectedResult.addAll(expectedFilesForTable1);
        expectedResult.addAll(expectedFilesForTable2);

        assertThat(result, containsInAnyOrder(expectedResult.toArray()));
    }

    @Test
    public void copyObjectsShouldCopyGivenObjectsFromSourceToDestinationBucketsWhenDeleteCopiedFilesIsFalse() {
        List<String> objectKeys = new ArrayList<>();
        objectKeys.add("file1.parquet");
        objectKeys.add("file2.parquet");
        objectKeys.add("file3.parquet");
        objectKeys.add("file4.parquet");

        Set<String> failedObjects = undertest.copyObjects(objectKeys, SOURCE_BUCKET, SOURCE_PREFIX, DESTINATION_BUCKET, DESTINATION_PREFIX,false);

        verify(mockS3Client, times(objectKeys.size())).copyObject(any(), any(), eq(SOURCE_BUCKET), eq(DESTINATION_BUCKET));

        assertThat(failedObjects, is(empty()));
    }

    @Test
    public void copyObjectsShouldReturnListOfFailedObjectsWhenDeleteCopiedFilesIsFalse() {
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
    public void copyObjectsShouldCopyAndDeleteGivenObjectsFromSourceToDestinationBucketsWhenDeleteCopiedFilesIsTrue() {
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
    public void copyObjectsShouldReturnListOfObjectsWhichFailedToBeCopiedWhenDeleteCopiedFilesIsTrue() {
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
    public void copyObjectsShouldReturnListOfObjectsWhichFailedToBeDeletedWhenDeleteCopiedFilesIsTrue() {
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
    public void deleteObjectsShouldDeleteGivenObjectsFromS3() {
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
    public void deleteObjectsShouldReturnListOfObjectsWhichFailedDeletion() {
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
}