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
import uk.gov.justice.digital.exception.ConfigServiceException;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.S3FileService;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static uk.gov.justice.digital.common.RegexPatterns.parquetFileRegex;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class S3FileTransferJobTest extends BaseSparkTest {

    @Mock
    private ConfigService mockConfigService;
    @Mock
    private JobArguments mockJobArguments;
    @Mock
    private S3FileService mockS3FileService;

    private static final String TEST_CONFIG_KEY = "some-config-key";
    private final static String SOURCE_BUCKET = "source-bucket";
    private final static String SOURCE_PREFIX = "source-prefix";
    private final static String DESTINATION_BUCKET = "destination-bucket";
    private final static String DESTINATION_PREFIX = "destination-prefix";

    private static final long RETENTION_AMOUNT = 2L;
    private static final Duration retentionPeriod = Duration.of(RETENTION_AMOUNT, ChronoUnit.DAYS);

    private S3FileTransferJob underTest;

    @BeforeEach
    public void setup() {
        reset(mockConfigService, mockS3FileService, mockJobArguments);

        underTest = new S3FileTransferJob(
                mockConfigService,
                mockS3FileService,
                mockJobArguments
        );
    }

    @Test
    public void shouldMoveFilesBelongingToGivenConfiguration() {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema_1", "table_1"),
                ImmutablePair.of("schema_2", "table_2")
        );

        List<String> objectsToMove = new ArrayList<>();
        objectsToMove.add("schema_1/table_1/file_1.parquet");
        objectsToMove.add("schema_1/table_1/file_2.parquet");
        objectsToMove.add("schema_2/table_2/file_3.parquet");

        when(mockJobArguments.getOptionalConfigKey()).thenReturn(Optional.of(TEST_CONFIG_KEY));
        when(mockJobArguments.getTransferSourceBucket()).thenReturn(SOURCE_BUCKET);
        when(mockJobArguments.getSourcePrefix()).thenReturn(SOURCE_PREFIX);
        when(mockJobArguments.getTransferDestinationBucket()).thenReturn(DESTINATION_BUCKET);
        when(mockJobArguments.getTransferDestinationPrefix()).thenReturn(DESTINATION_PREFIX);
        when(mockJobArguments.getFileTransferRetentionPeriod()).thenReturn(retentionPeriod);
        when(mockJobArguments.getFileTransferDeleteCopiedFilesFlag()).thenReturn(true);
        when(mockJobArguments.getAllowedS3FileNameRegex()).thenReturn(parquetFileRegex);

        when(mockConfigService.getConfiguredTables(TEST_CONFIG_KEY)).thenReturn(configuredTables);

        when(mockS3FileService.listFilesBeforePeriod(SOURCE_BUCKET, SOURCE_PREFIX, configuredTables, parquetFileRegex, retentionPeriod))
                .thenReturn(objectsToMove.stream().map(FileLastModifiedDate::new).collect(Collectors.toList()));

        when(mockS3FileService.copyObjects(objectsToMove, SOURCE_BUCKET, SOURCE_PREFIX, DESTINATION_BUCKET, DESTINATION_PREFIX, true))
                .thenReturn(Collections.emptySet());

        assertDoesNotThrow(() -> underTest.run());
    }

    @Test
    public void shouldMoveAllFilesWhenNoConfigurationIsGiven() {
        List<String> objectsToMove = new ArrayList<>();
        objectsToMove.add("schema_1/table_1/file_1.parquet");
        objectsToMove.add("schema_1/table_1/file_2.parquet");
        objectsToMove.add("schema_2/table_2/file_3.parquet");

        when(mockJobArguments.getOptionalConfigKey()).thenReturn(Optional.empty());
        when(mockJobArguments.getTransferSourceBucket()).thenReturn(SOURCE_BUCKET);
        when(mockJobArguments.getSourcePrefix()).thenReturn(SOURCE_PREFIX);
        when(mockJobArguments.getTransferDestinationBucket()).thenReturn(DESTINATION_BUCKET);
        when(mockJobArguments.getTransferDestinationPrefix()).thenReturn(DESTINATION_PREFIX);
        when(mockJobArguments.getFileTransferRetentionPeriod()).thenReturn(retentionPeriod);
        when(mockJobArguments.getFileTransferDeleteCopiedFilesFlag()).thenReturn(true);
        when(mockJobArguments.getAllowedS3FileNameRegex()).thenReturn(parquetFileRegex);

        when(mockS3FileService.listFiles(SOURCE_BUCKET, SOURCE_PREFIX, parquetFileRegex, retentionPeriod))
                .thenReturn(objectsToMove.stream().map(FileLastModifiedDate::new).collect(Collectors.toList()));

        when(mockS3FileService.copyObjects(objectsToMove, SOURCE_BUCKET, SOURCE_PREFIX, DESTINATION_BUCKET, DESTINATION_PREFIX, true))
                .thenReturn(Collections.emptySet());

        assertDoesNotThrow(() -> underTest.run());
    }

    @Test
    public void shouldExitWithFailureStatusWhenThereIsFailureMovingSomeFiles() throws Exception {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema_1", "table_1"),
                ImmutablePair.of("schema_2", "table_2")
        );

        List<String> objectsToMove = new ArrayList<>();
        objectsToMove.add("schema_1/table_1/file_1.parquet");
        objectsToMove.add("schema_1/table_1/file_2.parquet");
        objectsToMove.add("schema_2/table_2/file_3.parquet");

        Set<String> failedFiles = new HashSet<>();
        failedFiles.add("schema_2/table_2/file_3.parquet");

        when(mockJobArguments.getConfigKey()).thenReturn(TEST_CONFIG_KEY);
        when(mockJobArguments.getTransferSourceBucket()).thenReturn(SOURCE_BUCKET);
        when(mockJobArguments.getSourcePrefix()).thenReturn(SOURCE_PREFIX);
        when(mockJobArguments.getTransferDestinationBucket()).thenReturn(DESTINATION_BUCKET);
        when(mockJobArguments.getTransferDestinationPrefix()).thenReturn(DESTINATION_PREFIX);
        when(mockJobArguments.getFileTransferRetentionPeriod()).thenReturn(retentionPeriod);
        when(mockJobArguments.getFileTransferDeleteCopiedFilesFlag()).thenReturn(true);
        when(mockJobArguments.getAllowedS3FileNameRegex()).thenReturn(parquetFileRegex);

        when(mockConfigService.getConfiguredTables(TEST_CONFIG_KEY)).thenReturn(configuredTables);

        when(mockS3FileService.listFilesBeforePeriod(SOURCE_BUCKET, SOURCE_PREFIX, configuredTables, parquetFileRegex, retentionPeriod))
                .thenReturn(objectsToMove.stream().map(FileLastModifiedDate::new).collect(Collectors.toList()));

        when(mockS3FileService.copyObjects(objectsToMove, SOURCE_BUCKET, SOURCE_PREFIX, DESTINATION_BUCKET, DESTINATION_PREFIX, true))
                .thenReturn(failedFiles);

        SystemLambda.catchSystemExit(() -> underTest.run());
    }

    @Test
    public void shouldExitWithFailureStatusWhenConfigServiceThrowsAnException() throws Exception {
        when(mockJobArguments.getOptionalConfigKey()).thenReturn(Optional.of(TEST_CONFIG_KEY));
        when(mockConfigService.getConfiguredTables(TEST_CONFIG_KEY)).thenThrow(new ConfigServiceException("config error"));

        SystemLambda.catchSystemExit(() -> underTest.run());

        verifyNoInteractions(mockS3FileService);
    }
}
