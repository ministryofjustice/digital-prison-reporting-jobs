package uk.gov.justice.digital.job;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.service.CheckpointReaderService;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.S3FileService;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

import static uk.gov.justice.digital.common.RegexPatterns.parquetFileRegex;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.eq;

@ExtendWith(MockitoExtension.class)
class RawFileArchiveJobTest extends BaseSparkTest {

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

    private static final String SOURCE_BUCKET = "source-bucket";
    private static final String DESTINATION_BUCKET = "destination-bucket";
    private static final String CONFIG_KEY = "some-config";
    private static final String COMMITTED_FILE_1 = "committed-file-1";
    private static final String COMMITTED_FILE_2 = "committed-file-2";
    private static final String COMMITTED_FILE_3 = "committed-file-3";
    private static final String COMMITTED_OLD_FILE_4 = "committed-old-file-4";
    private static final String UNCOMMITTED_FILE = "uncommitted-file";
    private static final String UNCOMMITTED_OLD_FILE = "uncommitted-old-file";
    private static final Duration retentionPeriod = Duration.ofDays(1L);

    private RawFileArchiveJob underTest;

    @BeforeEach
    public void setup() {
        reset(mockConfigService, mockS3Service, mockCheckpointReaderService, mockJobArguments);

        underTest = new RawFileArchiveJob(mockConfigService, mockS3Service, mockCheckpointReaderService, mockJobArguments);
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldArchiveCommittedRawFilesForConfiguredTables() {
        ImmutablePair<String, String> configuredTable1 = ImmutablePair.of("source", "table-1");
        ImmutablePair<String, String> configuredTable2 = ImmutablePair.of("source", "table-2");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(configuredTable1, configuredTable2);

        List<String> rawFiles = new ArrayList<>();
        rawFiles.add(COMMITTED_FILE_1);
        rawFiles.add(COMMITTED_FILE_2);
        rawFiles.add(COMMITTED_FILE_3);
        rawFiles.add(COMMITTED_OLD_FILE_4);
        rawFiles.add(UNCOMMITTED_FILE);
        rawFiles.add(UNCOMMITTED_OLD_FILE);

        List<String> oldRawFiles = new ArrayList<>();
        oldRawFiles.add(COMMITTED_OLD_FILE_4);
        oldRawFiles.add(UNCOMMITTED_OLD_FILE);

        Set<String> committedFilesTable1 = new HashSet<>();
        committedFilesTable1.add(COMMITTED_FILE_1);
        committedFilesTable1.add(COMMITTED_FILE_2);

        Set<String> committedFilesTable2 = new HashSet<>();
        committedFilesTable2.add(COMMITTED_FILE_3);
        committedFilesTable2.add(COMMITTED_OLD_FILE_4);

        when(mockJobArguments.getTransferSourceBucket()).thenReturn(SOURCE_BUCKET);
        when(mockJobArguments.getTransferDestinationBucket()).thenReturn(DESTINATION_BUCKET);
        when(mockJobArguments.getRawFileRetentionPeriod()).thenReturn(retentionPeriod);
        when(mockJobArguments.getConfigKey()).thenReturn(CONFIG_KEY);

        when(mockConfigService.getConfiguredTables(CONFIG_KEY)).thenReturn(configuredTables);
        when(mockCheckpointReaderService.getCommittedFilesForTable(configuredTable1)).thenReturn(committedFilesTable1);
        when(mockCheckpointReaderService.getCommittedFilesForTable(configuredTable2)).thenReturn(committedFilesTable2);
        when(mockS3Service.listFilesForConfig(SOURCE_BUCKET, "", configuredTables, parquetFileRegex, retentionPeriod))
                .thenReturn(oldRawFiles);
        when(mockS3Service.listFilesForConfig(SOURCE_BUCKET, "", configuredTables, parquetFileRegex, Duration.ZERO))
                .thenReturn(new ArrayList<String>(CollectionUtils.subtract(rawFiles, oldRawFiles)));
        when(mockS3Service.deleteObjects(filesToDeleteCaptor.capture(), eq(SOURCE_BUCKET)))
                .thenReturn(Collections.emptySet());
        when(mockS3Service.copyObjects(filesToArchiveCaptor.capture(), eq(SOURCE_BUCKET), eq(""), eq(DESTINATION_BUCKET), eq(""), eq(false)))
                .thenReturn(Collections.emptySet());

        underTest.run();

        List<String> expectedFilesToArchive = new ArrayList<>();
        expectedFilesToArchive.add(COMMITTED_FILE_1);
        expectedFilesToArchive.add(COMMITTED_FILE_2);
        expectedFilesToArchive.add(COMMITTED_FILE_3);
        assertThat(filesToArchiveCaptor.getValue(), containsInAnyOrder(expectedFilesToArchive.toArray()));

        List<String> expectedFilesToDelete = new ArrayList<>();
        expectedFilesToDelete.add(COMMITTED_OLD_FILE_4);
        assertThat(filesToDeleteCaptor.getValue(), containsInAnyOrder(expectedFilesToDelete.toArray()));
    }

    @Test
    void shouldCompleteSuccessfullyWhenThereAreNoRawFilesToArchive() {
        ImmutablePair<String, String> configuredTable = ImmutablePair.of("source", "table-1");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(configuredTable);

        Set<String> committedFilesTable = new HashSet<>();
        committedFilesTable.add(COMMITTED_FILE_1);
        committedFilesTable.add(COMMITTED_FILE_2);

        when(mockJobArguments.getTransferSourceBucket()).thenReturn(SOURCE_BUCKET);
        when(mockJobArguments.getTransferDestinationBucket()).thenReturn(DESTINATION_BUCKET);
        when(mockJobArguments.getRawFileRetentionPeriod()).thenReturn(retentionPeriod);
        when(mockJobArguments.getConfigKey()).thenReturn(CONFIG_KEY);

        when(mockConfigService.getConfiguredTables(CONFIG_KEY)).thenReturn(configuredTables);
        when(mockCheckpointReaderService.getCommittedFilesForTable(configuredTable)).thenReturn(committedFilesTable);
        when(mockS3Service.listFilesForConfig(SOURCE_BUCKET, "", configuredTables, parquetFileRegex, retentionPeriod))
                .thenReturn(Collections.emptyList());
        when(mockS3Service.listFilesForConfig(SOURCE_BUCKET, "", configuredTables, parquetFileRegex, Duration.ZERO))
                .thenReturn(Collections.emptyList());
        when(mockS3Service.deleteObjects(filesToDeleteCaptor.capture(), eq(SOURCE_BUCKET)))
                .thenReturn(Collections.emptySet());
        when(mockS3Service.copyObjects(filesToArchiveCaptor.capture(), eq(SOURCE_BUCKET), eq(""), eq(DESTINATION_BUCKET), eq(""), eq(false)))
                .thenReturn(Collections.emptySet());

        underTest.run();

        assertThat(filesToDeleteCaptor.getValue(), is(empty()));
        assertThat(filesToArchiveCaptor.getValue(), is(empty()));
    }
}
