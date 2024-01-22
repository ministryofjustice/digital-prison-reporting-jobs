package uk.gov.justice.digital.job;

import com.github.stefanbirkner.systemlambda.SystemLambda;
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
import uk.gov.justice.digital.exception.ConfigServiceException;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.S3FileService;

import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class S3DataDeletionJobTest extends BaseSparkTest {

    private static final String TEST_CONFIG_KEY = "some-config-key";

    @Mock
    private ConfigService mockConfigService;
    @Mock
    private JobArguments mockJobArguments;
    @Mock
    private S3FileService mockS3FileService;
    @Captor
    ArgumentCaptor<String> listObjectsBucketCaptor;
    @Captor
    ArgumentCaptor<String> deleteObjectsBucketCaptor;

    private final static ImmutableSet<String> bucketsToDeleteFrom = ImmutableSet
            .of("bucket-to-delete-from-1", "bucket-to-delete-from-2");

    private S3DataDeletionJob underTest;

    @BeforeEach
    public void setup() {
        reset(mockConfigService, mockS3FileService, mockJobArguments);

        underTest = new S3DataDeletionJob(
                mockConfigService,
                mockS3FileService,
                mockJobArguments
        );
    }

    @Test
    public void shouldDeleteFilesBelongingToGivenConfiguration() {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema_1", "table_1"),
                ImmutablePair.of("schema_2", "table_2")
        );

        List<String> objectsToDelete = new ArrayList<>();
        objectsToDelete.add("schema_1/table_1/file_1.parquet");
        objectsToDelete.add("schema_1/table_1/file_2.parquet");
        objectsToDelete.add("schema_2/table_2/file_3.parquet");

        when(mockJobArguments.getConfigKey()).thenReturn(TEST_CONFIG_KEY);
        when(mockJobArguments.getBucketsToDeleteFilesFrom()).thenReturn(bucketsToDeleteFrom);
        when(mockConfigService.getConfiguredTables(TEST_CONFIG_KEY)).thenReturn(configuredTables);

        when(mockS3FileService.listParquetFilesForConfig(listObjectsBucketCaptor.capture(), eq(configuredTables), eq(0L)))
                .thenReturn(objectsToDelete);

        when(mockS3FileService.deleteObjects(eq(objectsToDelete), deleteObjectsBucketCaptor.capture()))
                .thenReturn(Collections.emptySet());

        assertDoesNotThrow(() -> underTest.run());

        assertThat(listObjectsBucketCaptor.getAllValues(), containsInAnyOrder(bucketsToDeleteFrom.toArray()));
        assertThat(deleteObjectsBucketCaptor.getAllValues(), containsInAnyOrder(bucketsToDeleteFrom.toArray()));
    }

    @Test
    public void shouldFailWhenNoConfigurationIsGiven() throws Exception {
        when(mockJobArguments.getConfigKey()).thenThrow(new IllegalStateException("error"));

        SystemLambda.catchSystemExit(() -> underTest.run());
    }

    @Test
    public void shouldExitWithFailureStatusWhenThereIsFailureDeletingSomeFiles() throws Exception {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(
                ImmutablePair.of("schema_1", "table_1"),
                ImmutablePair.of("schema_2", "table_2")
        );

        List<String> objectsToDelete = new ArrayList<>();
        objectsToDelete.add("schema_1/table_1/file_1.parquet");
        objectsToDelete.add("schema_1/table_1/file_2.parquet");
        objectsToDelete.add("schema_2/table_2/file_3.parquet");

        Set<String> failedFiles = new HashSet<>();
        failedFiles.add("schema_2/table_2/file_3.parquet");

        when(mockJobArguments.getConfigKey()).thenReturn(TEST_CONFIG_KEY);
        when(mockJobArguments.getBucketsToDeleteFilesFrom()).thenReturn(bucketsToDeleteFrom);
        when(mockConfigService.getConfiguredTables(TEST_CONFIG_KEY)).thenReturn(configuredTables);

        when(mockS3FileService.listParquetFilesForConfig(listObjectsBucketCaptor.capture(), eq(configuredTables), eq(0L)))
                .thenReturn(objectsToDelete);

        when(mockS3FileService.deleteObjects(eq(objectsToDelete), deleteObjectsBucketCaptor.capture())).thenReturn(failedFiles);

        SystemLambda.catchSystemExit(() -> underTest.run());

        assertThat(listObjectsBucketCaptor.getAllValues(), containsInAnyOrder(bucketsToDeleteFrom.toArray()));
        assertThat(deleteObjectsBucketCaptor.getAllValues(), containsInAnyOrder(bucketsToDeleteFrom.toArray()));
    }

    @Test
    public void shouldExitWithFailureStatusWhenConfigServiceThrowsAnException() throws Exception {
        when(mockJobArguments.getConfigKey()).thenReturn(TEST_CONFIG_KEY);
        when(mockConfigService.getConfiguredTables(TEST_CONFIG_KEY)).thenThrow(new ConfigServiceException("config error"));

        SystemLambda.catchSystemExit(() -> underTest.run());

        verifyNoInteractions(mockS3FileService);
    }
}
