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
import uk.gov.justice.digital.exception.ConfigServiceException;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.S3FileService;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

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
    private final static String DESTINATION_BUCKET = "destination-bucket";

    private static final long RETENTION_DAYS = 2L;

    private static final ImmutableSet<String> parquetFileExtension = ImmutableSet.of(".parquet");

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
        when(mockJobArguments.getTransferDestinationBucket()).thenReturn(DESTINATION_BUCKET);
        when(mockJobArguments.getFileTransferRetentionDays()).thenReturn(RETENTION_DAYS);
        when(mockJobArguments.getFileTransferDeleteCopiedFilesFlag()).thenReturn(true);
        when(mockJobArguments.getAllowedS3FileExtensions()).thenReturn(parquetFileExtension);

        when(mockConfigService.getConfiguredTables(TEST_CONFIG_KEY)).thenReturn(configuredTables);

        when(mockS3FileService.listFilesForConfig(SOURCE_BUCKET, configuredTables, parquetFileExtension, RETENTION_DAYS))
                .thenReturn(objectsToMove);

        when(mockS3FileService.copyObjects(objectsToMove, SOURCE_BUCKET, DESTINATION_BUCKET, true))
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
        when(mockJobArguments.getTransferDestinationBucket()).thenReturn(DESTINATION_BUCKET);
        when(mockJobArguments.getFileTransferRetentionDays()).thenReturn(RETENTION_DAYS);
        when(mockJobArguments.getFileTransferDeleteCopiedFilesFlag()).thenReturn(true);
        when(mockJobArguments.getAllowedS3FileExtensions()).thenReturn(parquetFileExtension);

        when(mockS3FileService.listFiles(SOURCE_BUCKET, parquetFileExtension, RETENTION_DAYS))
                .thenReturn(objectsToMove);

        when(mockS3FileService.copyObjects(objectsToMove, SOURCE_BUCKET, DESTINATION_BUCKET, true))
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
        when(mockJobArguments.getTransferDestinationBucket()).thenReturn(DESTINATION_BUCKET);
        when(mockJobArguments.getFileTransferRetentionDays()).thenReturn(RETENTION_DAYS);
        when(mockJobArguments.getFileTransferDeleteCopiedFilesFlag()).thenReturn(true);
        when(mockJobArguments.getAllowedS3FileExtensions()).thenReturn(parquetFileExtension);

        when(mockConfigService.getConfiguredTables(TEST_CONFIG_KEY)).thenReturn(configuredTables);

        when(mockS3FileService.listFilesForConfig(SOURCE_BUCKET, configuredTables, parquetFileExtension, RETENTION_DAYS))
                .thenReturn(objectsToMove);

        when(mockS3FileService.copyObjects(objectsToMove, SOURCE_BUCKET, DESTINATION_BUCKET, true))
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
