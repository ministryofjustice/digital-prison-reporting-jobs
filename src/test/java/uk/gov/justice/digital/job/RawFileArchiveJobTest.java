package uk.gov.justice.digital.job;

import com.google.common.collect.ImmutableSet;
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

    private final static String SOURCE_BUCKET = "source-bucket";
    private final static String DESTINATION_BUCKET = "destination-bucket";
    private final static String CONFIG_KEY = "some-config";
    private final static String COMMITTED_FILE_1 = "committed-file-1";
    private final static String COMMITTED_FILE_2 = "committed-file-2";
    private final static String COMMITTED_FILE_3 = "committed-file-3";
    private final static String COMMITTED_FILE_4 = "committed-file-4";
    private final static String UNCOMMITTED_FILE = "uncommitted-file";

    private RawFileArchiveJob underTest;

    @BeforeEach
    public void setup() {
        reset(mockConfigService, mockS3Service, mockCheckpointReaderService, mockJobArguments);

        underTest = new RawFileArchiveJob(mockConfigService, mockS3Service, mockCheckpointReaderService, mockJobArguments);
    }

    @Test
    void shouldArchiveCommittedRawFilesForConfiguredTables() {
        ImmutablePair<String, String> configuredTable1 = ImmutablePair.of("source", "table-1");
        ImmutablePair<String, String> configuredTable2 = ImmutablePair.of("source", "table-2");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(configuredTable1, configuredTable2);

        List<String> rawFiles = new ArrayList<>();
        rawFiles.add(COMMITTED_FILE_1);
        rawFiles.add(COMMITTED_FILE_2);
        rawFiles.add(COMMITTED_FILE_3);
        rawFiles.add(COMMITTED_FILE_4);
        rawFiles.add(UNCOMMITTED_FILE);

        Set<String> committedFilesTable1 = new HashSet<>();
        committedFilesTable1.add(COMMITTED_FILE_1);
        committedFilesTable1.add(COMMITTED_FILE_2);

        Set<String> committedFilesTable2 = new HashSet<>();
        committedFilesTable2.add(COMMITTED_FILE_3);
        committedFilesTable2.add(COMMITTED_FILE_4);

        when(mockJobArguments.getTransferSourceBucket()).thenReturn(SOURCE_BUCKET);
        when(mockJobArguments.getTransferDestinationBucket()).thenReturn(DESTINATION_BUCKET);
        when(mockJobArguments.getConfigKey()).thenReturn(CONFIG_KEY);

        when(mockConfigService.getConfiguredTables(CONFIG_KEY)).thenReturn(configuredTables);
        when(mockCheckpointReaderService.getCommittedFilesForTable(configuredTable1)).thenReturn(committedFilesTable1);
        when(mockCheckpointReaderService.getCommittedFilesForTable(configuredTable2)).thenReturn(committedFilesTable2);
        when(mockS3Service.listFilesForConfig(SOURCE_BUCKET, "", configuredTables, ImmutableSet.of(".parquet"), Duration.ZERO))
                .thenReturn(rawFiles);
        when(mockS3Service.copyObjects(filesToArchiveCaptor.capture(), eq(SOURCE_BUCKET), eq(""), eq(DESTINATION_BUCKET), eq(""), eq(true)))
                .thenReturn(Collections.emptySet());

        underTest.run();

        List<String> expectedArchivedFiles = new ArrayList<>();
        expectedArchivedFiles.add(COMMITTED_FILE_1);
        expectedArchivedFiles.add(COMMITTED_FILE_2);
        expectedArchivedFiles.add(COMMITTED_FILE_3);
        expectedArchivedFiles.add(COMMITTED_FILE_4);
        assertThat(filesToArchiveCaptor.getValue(), containsInAnyOrder(expectedArchivedFiles.toArray()));
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
        when(mockJobArguments.getConfigKey()).thenReturn(CONFIG_KEY);

        when(mockConfigService.getConfiguredTables(CONFIG_KEY)).thenReturn(configuredTables);
        when(mockCheckpointReaderService.getCommittedFilesForTable(configuredTable)).thenReturn(committedFilesTable);
        when(mockS3Service.listFilesForConfig(SOURCE_BUCKET, "", configuredTables, ImmutableSet.of(".parquet"), Duration.ZERO))
                .thenReturn(Collections.emptyList());
        when(mockS3Service.copyObjects(filesToArchiveCaptor.capture(), eq(SOURCE_BUCKET), eq(""), eq(DESTINATION_BUCKET), eq(""), eq(true)))
                .thenReturn(Collections.emptySet());

        underTest.run();

        assertThat(filesToArchiveCaptor.getValue(), is(empty()));
    }
}
