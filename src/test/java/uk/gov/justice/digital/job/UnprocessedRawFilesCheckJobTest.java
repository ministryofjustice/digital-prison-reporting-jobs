package uk.gov.justice.digital.job;

import com.github.stefanbirkner.systemlambda.SystemLambda;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.FileLastModifiedDate;
import uk.gov.justice.digital.service.CheckpointReaderService;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.S3FileService;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.RegexPatterns.parquetFileRegex;

@ExtendWith(MockitoExtension.class)
class UnprocessedRawFilesCheckJobTest extends BaseSparkTest {

    @Mock
    ConfigService mockConfigService;
    @Mock
    S3FileService mockS3Service;
    @Mock
    CheckpointReaderService mockCheckpointReaderService;
    @Mock
    JobArguments mockJobArguments;

    private final static String CONFIG_KEY = "some-config";
    private final static String SOURCE_BUCKET = "source-bucket";
    private final static String COMMITTED_FILE_1 = "committed-file-1";
    private final static String COMMITTED_FILE_2 = "committed-file-2";
    private final static String COMMITTED_FILE_3 = "committed-file-3";
    private final static String COMMITTED_FILE_4 = "committed-file-4";
    private final static String UNCOMMITTED_FILE = "uncommitted-file";

    private UnprocessedRawFilesCheckJob underTest;

    @BeforeEach
    public void setup() {
        reset(mockConfigService, mockS3Service, mockCheckpointReaderService, mockJobArguments);

        underTest = new UnprocessedRawFilesCheckJob(mockConfigService, mockS3Service, mockCheckpointReaderService, mockJobArguments);
    }

    @Test
    void shouldCompleteSuccessfullyWhenAllFilesHaveBeenCommitted() {
        ImmutablePair<String, String> configuredTable1 = ImmutablePair.of("source", "table-1");
        ImmutablePair<String, String> configuredTable2 = ImmutablePair.of("source", "table-2");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(configuredTable1, configuredTable2);

        List<FileLastModifiedDate> rawFiles = new ArrayList<>();
        rawFiles.add(new FileLastModifiedDate(COMMITTED_FILE_1));
        rawFiles.add(new FileLastModifiedDate(COMMITTED_FILE_2));
        rawFiles.add(new FileLastModifiedDate(COMMITTED_FILE_3));
        rawFiles.add(new FileLastModifiedDate(COMMITTED_FILE_4));

        Set<String> committedFilesTable1 = new HashSet<>();
        committedFilesTable1.add(COMMITTED_FILE_1);
        committedFilesTable1.add(COMMITTED_FILE_2);

        Set<String> committedFilesTable2 = new HashSet<>();
        committedFilesTable2.add(COMMITTED_FILE_3);
        committedFilesTable2.add(COMMITTED_FILE_4);

        when(mockJobArguments.orchestrationMaxAttempts()).thenReturn(1);
        when(mockJobArguments.orchestrationWaitIntervalSeconds()).thenReturn(1);
        when(mockJobArguments.getTransferSourceBucket()).thenReturn(SOURCE_BUCKET);
        when(mockJobArguments.getConfigKey()).thenReturn(CONFIG_KEY);
        when(mockJobArguments.getAllowedS3FileNameRegex()).thenReturn(parquetFileRegex);

        when(mockConfigService.getConfiguredTables(CONFIG_KEY)).thenReturn(configuredTables);
        when(mockCheckpointReaderService.getCommittedFilesForTable(configuredTable1)).thenReturn(committedFilesTable1);
        when(mockCheckpointReaderService.getCommittedFilesForTable(configuredTable2)).thenReturn(committedFilesTable2);
        when(mockS3Service.listFilesBeforePeriod(SOURCE_BUCKET, "", configuredTables, parquetFileRegex, Duration.ZERO))
                .thenReturn(rawFiles);

        assertDoesNotThrow(() -> underTest.run());
    }

    @Test
    void shouldFailIfThereAreUncommittedFilesAfterMaxAttempts() throws Exception {
        ImmutablePair<String, String> configuredTable1 = ImmutablePair.of("source", "table-1");
        ImmutablePair<String, String> configuredTable2 = ImmutablePair.of("source", "table-2");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(configuredTable1, configuredTable2);

        List<FileLastModifiedDate> rawFiles = new ArrayList<>();
        rawFiles.add(new FileLastModifiedDate(COMMITTED_FILE_1));
        rawFiles.add(new FileLastModifiedDate(COMMITTED_FILE_2));
        rawFiles.add(new FileLastModifiedDate(COMMITTED_FILE_3));
        rawFiles.add(new FileLastModifiedDate(COMMITTED_FILE_4));
        rawFiles.add(new FileLastModifiedDate(UNCOMMITTED_FILE));

        Set<String> committedFilesTable1 = new HashSet<>();
        committedFilesTable1.add(COMMITTED_FILE_1);
        committedFilesTable1.add(COMMITTED_FILE_2);

        Set<String> committedFilesTable2 = new HashSet<>();
        committedFilesTable2.add(COMMITTED_FILE_3);
        committedFilesTable2.add(COMMITTED_FILE_4);

        when(mockJobArguments.orchestrationMaxAttempts()).thenReturn(1);
        when(mockJobArguments.orchestrationWaitIntervalSeconds()).thenReturn(1);
        when(mockJobArguments.getTransferSourceBucket()).thenReturn(SOURCE_BUCKET);
        when(mockJobArguments.getConfigKey()).thenReturn(CONFIG_KEY);
        when(mockJobArguments.getAllowedS3FileNameRegex()).thenReturn(parquetFileRegex);

        when(mockConfigService.getConfiguredTables(CONFIG_KEY)).thenReturn(configuredTables);
        when(mockCheckpointReaderService.getCommittedFilesForTable(configuredTable1)).thenReturn(committedFilesTable1);
        when(mockCheckpointReaderService.getCommittedFilesForTable(configuredTable2)).thenReturn(committedFilesTable2);
        when(mockS3Service.listFilesBeforePeriod(SOURCE_BUCKET, "", configuredTables, parquetFileRegex, Duration.ZERO))
                .thenReturn(rawFiles);

        assertEquals(1, SystemLambda.catchSystemExit(() -> underTest.run()));
    }
}
