package uk.gov.justice.digital.service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
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
import uk.gov.justice.digital.client.s3.S3ObjectClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.FileLastModifiedDate;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;
import java.util.stream.Collectors;

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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doNothing;
import static uk.gov.justice.digital.client.s3.S3ObjectClient.DELIMITER;
import static uk.gov.justice.digital.test.Fixtures.fixedClock;
import static uk.gov.justice.digital.common.RegexPatterns.parquetFileRegex;
import static uk.gov.justice.digital.test.TestHelpers.givenConfiguredRetriesJobArgs;

@ExtendWith(MockitoExtension.class)
class S3FileServiceTest {

    private static final String SOURCE_BUCKET = "source-bucket";
    private static final String SOURCE_PREFIX = "source-prefix";
    private static final String DESTINATION_BUCKET = "destination-bucket";
    private static final String DESTINATION_PREFIX = "destination-prefix";
    private static final String TEST_CONFIG_KEY = "some-config-key";
    private static final long RETENTION_AMOUNT = 2L;
    private static final Duration period = Duration.of(RETENTION_AMOUNT, ChronoUnit.DAYS);

    @Mock
    private S3ObjectClient mockS3Client;
    @Mock
    private JobArguments mockJobArguments;
    @Captor
    ArgumentCaptor<List<String>> deleteObjectsArgCaptor;
    @Captor
    ArgumentCaptor<byte[]> saveObjectDataCaptor;

    private S3FileService undertest;

    @BeforeEach
    void setup() {
        reset(mockS3Client, mockJobArguments);
        when(mockJobArguments.fileTransferUseDefaultParallelism()).thenReturn(true);
        givenConfiguredRetriesJobArgs(1, mockJobArguments);
        undertest = new S3FileService(mockS3Client, fixedClock, mockJobArguments);
    }

    @Test
    void listFilesShouldReturnEmptyListWhenThereAreNoParquetFiles() {
        when(mockS3Client.getObjectsOlderThan(any(), any(), any(), any(), any())).thenReturn(Collections.emptyList());

        List<FileLastModifiedDate> result = undertest.listFiles(SOURCE_BUCKET, SOURCE_PREFIX, parquetFileRegex, period);

        assertThat(result, is(empty()));
    }

    @Test
    void listFilesShouldReturnListOfParquetFiles() {
        List<FileLastModifiedDate> expected = new ArrayList<>();
        expected.add(new FileLastModifiedDate("file1.parquet"));
        expected.add(new FileLastModifiedDate("file2.parquet"));
        expected.add(new FileLastModifiedDate("file3.parquet"));
        expected.add(new FileLastModifiedDate("file4.parquet"));

        when(mockS3Client.getObjectsOlderThan(SOURCE_BUCKET, SOURCE_PREFIX, parquetFileRegex, period, fixedClock))
                .thenReturn(expected);

        List<FileLastModifiedDate> result = undertest.listFiles(SOURCE_BUCKET, SOURCE_PREFIX, parquetFileRegex, period);

        assertThat(result, containsInAnyOrder(expected.toArray()));
    }

    @Test
    void listFilesShouldRetryWhenErrorOccursDuringListingOfFiles() {
        int numRetries = 2;
        givenConfiguredRetriesJobArgs(numRetries, mockJobArguments);
        doThrow(new AmazonS3Exception("s3 error")).when(mockS3Client).getObjectsOlderThan(any(), any(), any(), any(), any());

        S3FileService s3FileService = new S3FileService(mockS3Client, fixedClock, mockJobArguments);

        assertThrows(AmazonS3Exception.class, () -> s3FileService.listFiles(SOURCE_BUCKET, SOURCE_PREFIX, parquetFileRegex, period));

        verify(mockS3Client, times(numRetries)).getObjectsOlderThan(any(), any(), any(), any(), any());
    }

    @Test
    void listFilesBeforePeriodShouldReturnEmptyListWhenThereAreNoParquetFilesForConfiguredTables() {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema_1", "table_1"),
                ImmutablePair.of("schema_2", "table_2")
        );

        when(mockS3Client.getObjectsOlderThan(any(), any(), any(), any(), any())).thenReturn(Collections.emptyList());

        List<FileLastModifiedDate> result = undertest.listFilesBeforePeriod(SOURCE_BUCKET, SOURCE_PREFIX, configuredTables, parquetFileRegex, period);

        assertThat(result, is(empty()));
    }

    @Test
    void listFilesBeforePeriodShouldListFilesInFolderPrefix() {
        ImmutablePair<String, String> configuredTable = ImmutablePair.of("schema_1", "table_1");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(configuredTable);

        undertest.listFilesBeforePeriod(SOURCE_BUCKET, SOURCE_PREFIX, configuredTables, parquetFileRegex, period);

        String folder = SOURCE_PREFIX + DELIMITER + configuredTable.left + DELIMITER + configuredTable.right + DELIMITER;
        verify(mockS3Client).getObjectsOlderThan(eq(SOURCE_BUCKET), eq(folder), any(), eq(period), any());
    }

    @Test
    void listFilesBeforePeriodShouldListFilesWhenNoFolderPrefixIsGiven() {
        ImmutablePair<String, String> configuredTable = ImmutablePair.of("schema_1", "table_1");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(configuredTable);

        undertest.listFilesBeforePeriod(SOURCE_BUCKET, "", configuredTables, parquetFileRegex, period);

        String folder = configuredTable.left + DELIMITER + configuredTable.right + DELIMITER;
        verify(mockS3Client).getObjectsOlderThan(eq(SOURCE_BUCKET), eq(folder), any(), eq(period), any());
    }

    @Test
    void listFilesBeforePeriodShouldReturnListOfParquetFilesRelatedToConfiguredTables() {
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
                parquetFileRegex,
                period,
                fixedClock))
                .thenReturn(createFileSummaries(expectedFilesForTable1));

        when(mockS3Client.getObjectsOlderThan(
                SOURCE_BUCKET,
                SOURCE_PREFIX + DELIMITER + configuredTable2 + DELIMITER,
                parquetFileRegex,
                period,
                fixedClock))
                .thenReturn(createFileSummaries(expectedFilesForTable2));


        List<String> result = undertest.listFilesBeforePeriod(SOURCE_BUCKET, SOURCE_PREFIX, configuredTables, parquetFileRegex, period)
                .stream().map(x -> x.key).collect(Collectors.toList());

        List<String> expectedResult = new ArrayList<>();
        expectedResult.addAll(expectedFilesForTable1);
        expectedResult.addAll(expectedFilesForTable2);

        assertThat(result, containsInAnyOrder(expectedResult.toArray()));
    }

    @Test
    void listFilesBeforePeriodShouldRetryWhenErrorOccursDuringListingOfFiles() {
        int numRetries = 2;
        givenConfiguredRetriesJobArgs(numRetries, mockJobArguments);
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(ImmutablePair.of("schema_1", "table_1"));
        doThrow(new AmazonS3Exception("s3 error")).when(mockS3Client).getObjectsOlderThan(any(), any(), any(), any(), any());

        S3FileService s3FileService = new S3FileService(mockS3Client, fixedClock, mockJobArguments);

        assertThrows(AmazonS3Exception.class, () -> s3FileService.listFilesBeforePeriod(SOURCE_BUCKET, SOURCE_PREFIX, configuredTables, parquetFileRegex, period));

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
    void shouldUseGivenFileTransferParallelismWhenTheArgumentToUseDefaultParallelismIsFalse() {
        when(mockJobArguments.fileTransferUseDefaultParallelism()).thenReturn(false);

        new S3FileService(mockS3Client, fixedClock, mockJobArguments);

        verify(mockJobArguments, times(1)).getFileTransferParallelism();
    }

    @Test
    void shouldNotUseGivenFileTransferParallelismWhenTheArgumentToUseDefaultParallelismIsTrue() {
        when(mockJobArguments.fileTransferUseDefaultParallelism()).thenReturn(true);

        new S3FileService(mockS3Client, fixedClock, mockJobArguments);

        verify(mockJobArguments, never()).getFileTransferParallelism();
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

        assertEquals(expectedFailedObjects, failedObjects);
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
        verify(mockS3Client).deleteObjects(deleteObjectsArgCaptor.capture(), eq(SOURCE_BUCKET));

        assertThat(failedObjects, is(empty()));
        assertThat(deleteObjectsArgCaptor.getValue(), containsInAnyOrder(objectKeys.toArray()));
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
        when(mockS3Client.deleteObjects(Collections.singletonList("file3.parquet"), SOURCE_BUCKET)).thenReturn(Collections.emptySet());

        Set<String> failedObjects = undertest.copyObjects(objectKeys, SOURCE_BUCKET, SOURCE_PREFIX, DESTINATION_BUCKET, DESTINATION_PREFIX, true);

        assertEquals(expectedFailedObjects, failedObjects);
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
        expectedFailedObjects.add("file3.parquet");
        expectedFailedObjects.add("file4.parquet");

        doNothing().when(mockS3Client).copyObject(any(), any(), any(), any());
        doThrow(new AmazonServiceException("failure")).when(mockS3Client).deleteObjects(deleteObjectsArgCaptor.capture(), eq(SOURCE_BUCKET));

        Set<String> failedObjects = undertest.copyObjects(objectKeys, SOURCE_BUCKET, SOURCE_PREFIX, DESTINATION_BUCKET, DESTINATION_PREFIX, true);

        assertEquals(expectedFailedObjects, failedObjects);
        assertThat(deleteObjectsArgCaptor.getValue(), containsInAnyOrder(expectedFailedObjects.toArray()));
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
        doThrow(new AmazonS3Exception("s3 error")).when(mockS3Client).deleteObjects(any(), any());

        S3FileService s3FileService = new S3FileService(mockS3Client, fixedClock, mockJobArguments);

        s3FileService.copyObjects(objectKeys, SOURCE_BUCKET, SOURCE_PREFIX, DESTINATION_BUCKET, DESTINATION_PREFIX, true);

        verify(mockS3Client, times(numRetries)).deleteObjects(any(), any());
    }

    @Test
    void deleteObjectsShouldDeleteGivenObjectsFromS3() {
        List<String> objectKeys = new ArrayList<>();
        objectKeys.add("file1.parquet");
        objectKeys.add("file2.parquet");
        objectKeys.add("file3.parquet");
        objectKeys.add("file4.parquet");

        Set<String> failedObjects = undertest.deleteObjects(objectKeys, SOURCE_BUCKET);

        verify(mockS3Client).deleteObjects(any(), eq(SOURCE_BUCKET));

        assertThat(failedObjects, is(empty()));
    }

    @Test
    void deleteObjectsShouldReturnListOfObjectsWhichFailedDeletion() {
        List<String> objectKeys = new ArrayList<>();
        objectKeys.add("file1.parquet");
        objectKeys.add("file2.parquet");
        objectKeys.add("file3.parquet");
        objectKeys.add("file4.parquet");

        Set<String> expectedFailedObjects = new HashSet<>();
        expectedFailedObjects.add("file2.parquet");
        expectedFailedObjects.add("file4.parquet");

        when(mockS3Client.deleteObjects(objectKeys, SOURCE_BUCKET)).thenReturn(expectedFailedObjects);

        Set<String> failedObjects = undertest.deleteObjects(objectKeys, SOURCE_BUCKET);

        assertThat(failedObjects, containsInAnyOrder(expectedFailedObjects.toArray()));
    }

    @Test
    void deleteObjectsShouldRetryWhenAnErrorOccursDeletingFile() {
        int numRetries = 2;
        List<String> objectKeys = Collections.singletonList("file1.parquet");
        givenConfiguredRetriesJobArgs(numRetries, mockJobArguments);
        doThrow(new AmazonS3Exception("s3 error")).when(mockS3Client).deleteObjects(any(), any());

        S3FileService s3FileService = new S3FileService(mockS3Client, fixedClock, mockJobArguments);

        s3FileService.deleteObjects(objectKeys, SOURCE_BUCKET);

        verify(mockS3Client, times(numRetries)).deleteObjects(any(), any());
    }

    @Test
    void getPreviousArchivedKeysShouldReturnEmptySetWhenAnErrorOccursReadingFile() {
        doThrow(new AmazonS3Exception("s3 error")).when(mockS3Client).getObject(SOURCE_BUCKET, "last-archived-files/" + TEST_CONFIG_KEY + "/archived.txt");

        Set<String> previouslyArchivedKeys = undertest.getPreviousArchivedKeys(SOURCE_BUCKET, TEST_CONFIG_KEY);

        assertThat(previouslyArchivedKeys, is(empty()));
    }

    @Test
    void getPreviousArchivedKeysShouldReturnSetOfPreviouslyArchivedKeys() {
        Set<String> expectedKeys = new HashSet<>();
        expectedKeys.add("file1");
        expectedKeys.add("file2");
        expectedKeys.add("file3");

        when(mockS3Client.getObject(SOURCE_BUCKET, "last-archived-files/" + TEST_CONFIG_KEY + "/archived.txt"))
                .thenReturn("file1 \nfile2\nfile3 ");

        Set<String> previouslyArchivedKeys = undertest.getPreviousArchivedKeys(SOURCE_BUCKET, TEST_CONFIG_KEY);

        assertThat(previouslyArchivedKeys, containsInAnyOrder(expectedKeys.toArray()));
    }

    @Test
    void getPreviousArchivedKeysShouldRetryWhenAnErrorOccursWhileReadingArchivedKeys() {
        int numRetries = 2;
        givenConfiguredRetriesJobArgs(numRetries, mockJobArguments);
        doThrow(new AmazonS3Exception("s3 error")).when(mockS3Client).getObject(any(), any());

        S3FileService s3FileService = new S3FileService(mockS3Client, fixedClock, mockJobArguments);
        s3FileService.getPreviousArchivedKeys(SOURCE_BUCKET, TEST_CONFIG_KEY);

        verify(mockS3Client, times(numRetries)).getObject(any(), any());
    }

    @Test
    void saveArchivedKeysShouldSavePreviouslyArchivedKeys() {
        List<String> keysToSave = new ArrayList<>();
        keysToSave.add("file1");
        keysToSave.add("file2");
        keysToSave.add("file3");

        String archivedKeysPath = "last-archived-files/" + TEST_CONFIG_KEY + "/archived.txt";
        doNothing().when(mockS3Client).saveObject(eq(SOURCE_BUCKET), eq(archivedKeysPath), saveObjectDataCaptor.capture(), eq(ContentType.DEFAULT_TEXT));

        undertest.saveArchivedKeys(SOURCE_BUCKET, TEST_CONFIG_KEY, keysToSave);

        assertEquals(
                "file1\nfile2\nfile3",
                IOUtils.toString(saveObjectDataCaptor.getValue(), StandardCharsets.UTF_8.name())
        );
    }

    @Test
    void saveArchivedKeysShouldRetryWhenAnErrorOccursWhileSavingArchivedKeys() {
        int numRetries = 2;
        givenConfiguredRetriesJobArgs(numRetries, mockJobArguments);
        doThrow(new AmazonS3Exception("s3 error")).when(mockS3Client).saveObject(any(), any(), any(), any());

        S3FileService s3FileService = new S3FileService(mockS3Client, fixedClock, mockJobArguments);
        s3FileService.saveArchivedKeys(SOURCE_BUCKET, TEST_CONFIG_KEY, Collections.singletonList("file1"));

        verify(mockS3Client, times(numRetries)).saveObject(any(), any(), any(), any());
    }

    @Test
    void saveArchivedKeysShouldNotSaveArchivedKeysWhenGivenAnEmptyListOfKeys() {
        S3FileService s3FileService = new S3FileService(mockS3Client, fixedClock, mockJobArguments);
        s3FileService.saveArchivedKeys(SOURCE_BUCKET, TEST_CONFIG_KEY, Collections.emptyList());

        verify(mockS3Client, never()).saveObject(any(), any(), any(), any());
    }

    @NotNull
    private static List<FileLastModifiedDate> createFileSummaries(List<String> keys) {
        return keys.stream().map(FileLastModifiedDate::new).collect(Collectors.toList());
    }
}