package uk.gov.justice.digital.job;

import com.ginsberg.junit.exit.ExpectSystemExitWithStatus;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.SparkTestBase;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.FileLastModifiedDate;
import uk.gov.justice.digital.service.CheckpointReaderService;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.S3FileService;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.reset;
import static uk.gov.justice.digital.common.RegexPatterns.parquetFileRegex;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static uk.gov.justice.digital.test.Fixtures.fixedClock;

@ExtendWith(MockitoExtension.class)
class RawFileArchiveJobTest extends SparkTestBase {

    @Mock
    ConfigService mockConfigService;
    @Mock
    S3FileService mockS3Service;
    @Mock
    CheckpointReaderService mockCheckpointReaderService;
    @Mock
    JobArguments mockJobArguments;
    @Captor
    ArgumentCaptor<ArrayList<String>> filesToArchiveCaptor;
    @Captor
    ArgumentCaptor<ArrayList<String>> filesToDeleteCaptor;
    @Captor
    ArgumentCaptor<List<String>> savedArchivedKeysCaptor;

    private static final String SOURCE_BUCKET = "source-bucket";
    private static final String DESTINATION_BUCKET = "destination-bucket";
    private static final String JOBS_BUCKET = "glue-jobs-bucket";
    private static final String CONFIG_KEY = "some-config";
    private static final String COMMITTED_FILE_1 = "committed-file-1";
    private static final String COMMITTED_FILE_2 = "committed-file-2";
    private static final String COMMITTED_FILE_3 = "committed-file-3";
    private static final String COMMITTED_OLD_FILE_4 = "committed-old-file-4";
    private static final String COMMITTED_OLD_FILE_5 = "committed-old-file-5";
    private static final String UNCOMMITTED_FILE = "uncommitted-file";
    private static final String UNCOMMITTED_OLD_FILE = "uncommitted-old-file";
    private static final String ARCHIVED_FILE_1 = "archived-file-1";
    private static final String ARCHIVED_FILE_2 = "archived-file-2";
    private static final Duration retentionPeriod = Duration.ofDays(1L);
    private static final LocalDateTime oldDateTime = LocalDateTime.now(fixedClock).minus(retentionPeriod).minusNanos(1L);

    private RawFileArchiveJob underTest;

    @BeforeEach
    void setup() {
        reset(mockConfigService, mockS3Service, mockCheckpointReaderService, mockJobArguments);

        underTest = new RawFileArchiveJob(mockConfigService, mockS3Service, mockCheckpointReaderService, fixedClock, mockJobArguments);
    }

    @Test
    void shouldDeleteRawFilesBelongingToConfiguredTablesWhichAreOlderThanRetentionPeriodAndHaveBeenCommitted() {
        ImmutablePair<String, String> configuredTable1 = ImmutablePair.of("source", "table-1");
        ImmutablePair<String, String> configuredTable2 = ImmutablePair.of("source", "table-2");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(configuredTable1, configuredTable2);

        List<FileLastModifiedDate> oldRawFiles = new ArrayList<>();
        oldRawFiles.add(new FileLastModifiedDate(COMMITTED_OLD_FILE_4, oldDateTime)); // This file is old and has been committed and will be deleted
        oldRawFiles.add(new FileLastModifiedDate(UNCOMMITTED_OLD_FILE, oldDateTime));

        List<FileLastModifiedDate> recentRawFiles = new ArrayList<>();
        recentRawFiles.add(new FileLastModifiedDate(COMMITTED_FILE_1));
        recentRawFiles.add(new FileLastModifiedDate(COMMITTED_FILE_2));
        recentRawFiles.add(new FileLastModifiedDate(COMMITTED_FILE_3));
        recentRawFiles.add(new FileLastModifiedDate(UNCOMMITTED_FILE));

        Set<String> committedFilesTable1 = new HashSet<>();
        committedFilesTable1.add(COMMITTED_FILE_1);
        committedFilesTable1.add(COMMITTED_FILE_2);

        Set<String> committedFilesTable2 = new HashSet<>();
        committedFilesTable2.add(COMMITTED_FILE_3);
        committedFilesTable2.add(COMMITTED_OLD_FILE_4);

        mockJobArguments();
        when(mockConfigService.getConfiguredTables(CONFIG_KEY)).thenReturn(configuredTables);
        when(mockCheckpointReaderService.getCommittedFilesForTable(configuredTable1)).thenReturn(committedFilesTable1);
        when(mockCheckpointReaderService.getCommittedFilesForTable(configuredTable2)).thenReturn(committedFilesTable2);
        when(mockS3Service.listFilesBeforePeriod(SOURCE_BUCKET, "", configuredTables, parquetFileRegex, Duration.ZERO))
                .thenReturn(Stream.concat(oldRawFiles.stream(), recentRawFiles.stream()).collect(Collectors.toList()));
        when(mockS3Service.deleteObjects(filesToDeleteCaptor.capture(), eq(SOURCE_BUCKET)))
                .thenReturn(Collections.emptySet());
        when(mockS3Service.copyObjects(any(), eq(SOURCE_BUCKET), eq(""), eq(DESTINATION_BUCKET), eq(""), eq(false)))
                .thenReturn(Collections.emptySet());

        underTest.run();

        List<String> expectedFilesToDelete = new ArrayList<>();
        expectedFilesToDelete.add(COMMITTED_OLD_FILE_4);

        assertThat(filesToDeleteCaptor.getValue(), containsInAnyOrder(expectedFilesToDelete.toArray()));
    }

    @Test
    void shouldArchiveRawFilesBelongingToConfiguredTablesWhichHaveNotAlreadyArchived() {
        ImmutablePair<String, String> configuredTable1 = ImmutablePair.of("source", "table-1");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(configuredTable1);

        List<FileLastModifiedDate> rawFiles = new ArrayList<>();
        rawFiles.add(new FileLastModifiedDate(COMMITTED_FILE_1));
        rawFiles.add(new FileLastModifiedDate(COMMITTED_FILE_2));
        rawFiles.add(new FileLastModifiedDate(COMMITTED_FILE_3));
        rawFiles.add(new FileLastModifiedDate(COMMITTED_OLD_FILE_4));
        rawFiles.add(new FileLastModifiedDate(UNCOMMITTED_FILE));
        rawFiles.add(new FileLastModifiedDate(ARCHIVED_FILE_2)); // Already archived file will be excluded from the list of files to archive
        rawFiles.add(new FileLastModifiedDate(UNCOMMITTED_OLD_FILE));

        Set<String> lastArchivedKeys = new HashSet<>();
        lastArchivedKeys.add(ARCHIVED_FILE_1);
        lastArchivedKeys.add(ARCHIVED_FILE_2);

        mockJobArguments();
        when(mockConfigService.getConfiguredTables(CONFIG_KEY)).thenReturn(configuredTables);
        when(mockCheckpointReaderService.getCommittedFilesForTable(configuredTable1)).thenReturn(Collections.emptySet());
        when(mockS3Service.listFilesBeforePeriod(SOURCE_BUCKET, "", configuredTables, parquetFileRegex, Duration.ZERO))
                .thenReturn(rawFiles);
        when(mockS3Service.getPreviousArchivedKeys(JOBS_BUCKET, CONFIG_KEY))
                .thenReturn(lastArchivedKeys);
        when(mockS3Service.deleteObjects(any(), eq(SOURCE_BUCKET)))
                .thenReturn(Collections.emptySet());
        when(mockS3Service.copyObjects(filesToArchiveCaptor.capture(), eq(SOURCE_BUCKET), eq(""), eq(DESTINATION_BUCKET), eq(""), eq(false)))
                .thenReturn(Collections.emptySet());
        doNothing().when(mockS3Service).saveArchivedKeys(eq(JOBS_BUCKET), eq(CONFIG_KEY), savedArchivedKeysCaptor.capture());

        underTest.run();

        List<String> expectedFilesToArchive = new ArrayList<>();
        expectedFilesToArchive.add(COMMITTED_FILE_1);
        expectedFilesToArchive.add(COMMITTED_FILE_2);
        expectedFilesToArchive.add(COMMITTED_FILE_3);
        expectedFilesToArchive.add(COMMITTED_OLD_FILE_4);
        expectedFilesToArchive.add(UNCOMMITTED_FILE);
        expectedFilesToArchive.add(UNCOMMITTED_OLD_FILE);

        List<String> expectedSavedArchivedKeys = new ArrayList<>(expectedFilesToArchive);
        expectedSavedArchivedKeys.add(ARCHIVED_FILE_1);
        expectedSavedArchivedKeys.add(ARCHIVED_FILE_2);

        assertThat(filesToArchiveCaptor.getValue(), containsInAnyOrder(expectedFilesToArchive.toArray()));
        assertThat(savedArchivedKeysCaptor.getValue(), containsInAnyOrder(expectedSavedArchivedKeys.toArray()));
    }

    @Test
    void shouldRemoveDeletedKeysFromListOfArchivedFilesToBeSaved() {
        ImmutablePair<String, String> configuredTable1 = ImmutablePair.of("source", "table-1");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(configuredTable1);

        List<FileLastModifiedDate> rawFiles = new ArrayList<>();
        rawFiles.add(new FileLastModifiedDate(COMMITTED_FILE_1));
        rawFiles.add(new FileLastModifiedDate(COMMITTED_OLD_FILE_4, oldDateTime));
        rawFiles.add(new FileLastModifiedDate(COMMITTED_OLD_FILE_5, oldDateTime));

        Set<String> alreadyArchivedFiles = new HashSet<>();
        alreadyArchivedFiles.add(ARCHIVED_FILE_1);
        alreadyArchivedFiles.add(ARCHIVED_FILE_2);
        alreadyArchivedFiles.add(COMMITTED_OLD_FILE_5);

        Set<String> committedFiles = rawFiles.stream().map(x -> x.key).collect(Collectors.toSet());

        mockJobArguments();
        when(mockConfigService.getConfiguredTables(CONFIG_KEY)).thenReturn(configuredTables);
        when(mockCheckpointReaderService.getCommittedFilesForTable(configuredTable1)).thenReturn(committedFiles);
        when(mockS3Service.listFilesBeforePeriod(SOURCE_BUCKET, "", configuredTables, parquetFileRegex, Duration.ZERO))
                .thenReturn(rawFiles);
        when(mockS3Service.getPreviousArchivedKeys(JOBS_BUCKET, CONFIG_KEY))
                .thenReturn(alreadyArchivedFiles);
        when(mockS3Service.deleteObjects(any(), eq(SOURCE_BUCKET)))
                .thenReturn(Collections.singleton(COMMITTED_OLD_FILE_4)); // This file failed to be deleted
        when(mockS3Service.copyObjects(filesToArchiveCaptor.capture(), eq(SOURCE_BUCKET), eq(""), eq(DESTINATION_BUCKET), eq(""), eq(false)))
                .thenReturn(Collections.emptySet());
        doNothing().when(mockS3Service).saveArchivedKeys(eq(JOBS_BUCKET), eq(CONFIG_KEY), savedArchivedKeysCaptor.capture());

        underTest.run();

        List<String> expectedFilesToArchive = new ArrayList<>();
        expectedFilesToArchive.add(COMMITTED_FILE_1);
        expectedFilesToArchive.add(COMMITTED_OLD_FILE_4);

        List<String> expectedSavedArchivedKeys = new ArrayList<>();
        expectedSavedArchivedKeys.add(ARCHIVED_FILE_1);
        expectedSavedArchivedKeys.add(ARCHIVED_FILE_2);
        expectedSavedArchivedKeys.add(COMMITTED_FILE_1);
        expectedSavedArchivedKeys.add(COMMITTED_OLD_FILE_4); // Since file failed deletion, it was not removed from saved archived keys

        assertThat(filesToArchiveCaptor.getValue(), containsInAnyOrder(expectedFilesToArchive.toArray()));
        assertThat(savedArchivedKeysCaptor.getValue(), containsInAnyOrder(expectedSavedArchivedKeys.toArray()));
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    @SuppressWarnings("java:S2699")
    void shouldFailWhenNotAllFilesWereArchived() {
        ImmutablePair<String, String> configuredTable1 = ImmutablePair.of("source", "table-1");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(configuredTable1);

        List<FileLastModifiedDate> rawFiles = new ArrayList<>();
        rawFiles.add(new FileLastModifiedDate(COMMITTED_FILE_1));

        mockJobArguments();
        when(mockConfigService.getConfiguredTables(CONFIG_KEY)).thenReturn(configuredTables);
        when(mockS3Service.listFilesBeforePeriod(SOURCE_BUCKET, "", configuredTables, parquetFileRegex, Duration.ZERO))
                .thenReturn(rawFiles);
        when(mockS3Service.getPreviousArchivedKeys(JOBS_BUCKET, CONFIG_KEY))
                .thenReturn(Collections.emptySet());
        when(mockS3Service.copyObjects(filesToArchiveCaptor.capture(), eq(SOURCE_BUCKET), eq(""), eq(DESTINATION_BUCKET), eq(""), eq(false)))
                .thenReturn(Collections.singleton(COMMITTED_FILE_1));

        underTest.run();
    }

    @Test
    void shouldCompleteSuccessfullyWhenThereAreNoRawFiles() {
        ImmutablePair<String, String> configuredTable = ImmutablePair.of("source", "table-1");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(configuredTable);

        Set<String> committedFilesTable = new HashSet<>();
        committedFilesTable.add(COMMITTED_FILE_1);
        committedFilesTable.add(COMMITTED_FILE_2);

        mockJobArguments();
        when(mockConfigService.getConfiguredTables(CONFIG_KEY)).thenReturn(configuredTables);
        when(mockCheckpointReaderService.getCommittedFilesForTable(configuredTable)).thenReturn(committedFilesTable);
        when(mockS3Service.listFilesBeforePeriod(SOURCE_BUCKET, "", configuredTables, parquetFileRegex, Duration.ZERO))
                .thenReturn(Collections.emptyList());
        when(mockS3Service.deleteObjects(filesToDeleteCaptor.capture(), eq(SOURCE_BUCKET)))
                .thenReturn(Collections.emptySet());
        when(mockS3Service.copyObjects(filesToArchiveCaptor.capture(), eq(SOURCE_BUCKET), eq(""), eq(DESTINATION_BUCKET), eq(""), eq(false)))
                .thenReturn(Collections.emptySet());

        underTest.run();

        assertThat(filesToDeleteCaptor.getValue(), is(empty()));
        assertThat(filesToArchiveCaptor.getValue(), is(empty()));
    }

    private void mockJobArguments() {
        when(mockJobArguments.getTransferSourceBucket()).thenReturn(SOURCE_BUCKET);
        when(mockJobArguments.getTransferDestinationBucket()).thenReturn(DESTINATION_BUCKET);
        when(mockJobArguments.getRawFileRetentionPeriod()).thenReturn(retentionPeriod);
        when(mockJobArguments.getConfigKey()).thenReturn(CONFIG_KEY);
        when(mockJobArguments.getJobsS3Bucket()).thenReturn(JOBS_BUCKET);
    }
}
