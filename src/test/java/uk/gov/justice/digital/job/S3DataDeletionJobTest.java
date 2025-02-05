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
import uk.gov.justice.digital.datahub.model.FileLastModifiedDate;
import uk.gov.justice.digital.exception.ConfigServiceException;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.S3FileService;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static uk.gov.justice.digital.common.RegexPatterns.parquetFileRegex;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class S3DataDeletionJobTest extends BaseSparkTest {

    private static final String TEST_CONFIG_KEY = "some-config-key";
    private static final String SOURCE_PREFIX = "source-prefix";

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
        when(mockJobArguments.getSourcePrefix()).thenReturn(SOURCE_PREFIX);
        when(mockJobArguments.getAllowedS3FileNameRegex()).thenReturn(parquetFileRegex);
        when(mockConfigService.getConfiguredTables(TEST_CONFIG_KEY)).thenReturn(configuredTables);

        when(mockS3FileService.listFilesBeforePeriod(
                listObjectsBucketCaptor.capture(),
                eq(SOURCE_PREFIX),
                eq(configuredTables),
                eq(parquetFileRegex),
                eq(Duration.ZERO)
        )).thenReturn(objectsToDelete.stream().map(FileLastModifiedDate::new).collect(Collectors.toList()));

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
        when(mockJobArguments.getSourcePrefix()).thenReturn(SOURCE_PREFIX);
        when(mockConfigService.getConfiguredTables(TEST_CONFIG_KEY)).thenReturn(configuredTables);
        when(mockJobArguments.getAllowedS3FileNameRegex()).thenReturn(parquetFileRegex);

        when(mockS3FileService.listFilesBeforePeriod(
                listObjectsBucketCaptor.capture(),
                eq(SOURCE_PREFIX),
                eq(configuredTables),
                eq(parquetFileRegex),
                eq(Duration.ZERO)
        )).thenReturn(objectsToDelete.stream().map(FileLastModifiedDate::new).collect(Collectors.toList()));

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
