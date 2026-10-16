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
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.FileLastModifiedDate;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.RawArchiveLocationService;
import uk.gov.justice.digital.service.S3FileService;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static com.ginsberg.junit.exit.assertions.SystemExitAssertion.assertThatCallsSystemExit;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.RegexPatterns.parquetFileRegex;

@ExtendWith(MockitoExtension.class)
class RawZoneReingestionFlushJobTest {

    private static final String SOURCE_BUCKET = "source-bucket";
    private static final String DESTINATION_BUCKET = "destination-bucket";
    private static final String CONFIG_KEY = "some-config";
    private static final String FILE_1 = "source/table-1/file-1.parquet";
    private static final String FILE_2 = "source/table-1/file-2.parquet";

    @Mock
    private ConfigService mockConfigService;
    @Mock
    private S3FileService mockS3Service;
    @Mock
    private JobArguments mockJobArguments;
    @Mock
    private RawArchiveLocationService mockRawArchiveLocationService;
    @Captor
    private ArgumentCaptor<List<String>> filesToArchiveCaptor;
    @Captor
    private ArgumentCaptor<Function<String, String>> destinationKeyMapperCaptor;

    private RawZoneReingestionFlushJob underTest;

    @BeforeEach
    void setup() {
        reset(mockConfigService, mockS3Service, mockJobArguments, mockRawArchiveLocationService);

        underTest = new RawZoneReingestionFlushJob(mockConfigService, mockS3Service, mockJobArguments, mockRawArchiveLocationService);
    }

    @Test
    void shouldArchiveAndDeleteEveryCurrentRawFileForTheConfiguredDomainRegardlessOfAge() {
        ImmutablePair<String, String> configuredTable = ImmutablePair.of("source", "table-1");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(configuredTable);

        List<FileLastModifiedDate> rawFiles = new ArrayList<>();
        rawFiles.add(new FileLastModifiedDate(FILE_1));
        rawFiles.add(new FileLastModifiedDate(FILE_2));

        mockJobArguments();
        when(mockConfigService.getConfiguredTables(CONFIG_KEY)).thenReturn(configuredTables);
        when(mockS3Service.listFilesBeforePeriod(SOURCE_BUCKET, "", configuredTables, parquetFileRegex, Duration.ZERO))
                .thenReturn(rawFiles);
        when(mockS3Service.copyObjects(filesToArchiveCaptor.capture(), eq(SOURCE_BUCKET), eq(DESTINATION_BUCKET), eq(true), any()))
                .thenReturn(Collections.emptySet());

        assertDoesNotThrow(() -> underTest.run());

        assertEquals(List.of(FILE_1, FILE_2), filesToArchiveCaptor.getValue().stream().sorted().toList());
    }

    @Test
    void shouldUseRawArchiveLocationServiceToBuildTheArchiveDestinationKey() {
        ImmutablePair<String, String> configuredTable = ImmutablePair.of("source", "table-1");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(configuredTable);

        mockJobArguments();
        when(mockConfigService.getConfiguredTables(CONFIG_KEY)).thenReturn(configuredTables);
        when(mockS3Service.listFilesBeforePeriod(SOURCE_BUCKET, "", configuredTables, parquetFileRegex, Duration.ZERO))
                .thenReturn(Collections.singletonList(new FileLastModifiedDate(FILE_1)));
        when(mockS3Service.copyObjects(any(), eq(SOURCE_BUCKET), eq(DESTINATION_BUCKET), eq(true), destinationKeyMapperCaptor.capture()))
                .thenReturn(Collections.emptySet());
        when(mockRawArchiveLocationService.applyVersionToKey(FILE_1)).thenReturn("source/table-1/v2/file-1.parquet");

        underTest.run();

        assertEquals("source/table-1/v2/file-1.parquet", destinationKeyMapperCaptor.getValue().apply(FILE_1));
    }

    @Test
    void shouldExitWithFailureStatusWhenNotAllFilesWereArchived() {
        ImmutablePair<String, String> configuredTable = ImmutablePair.of("source", "table-1");
        ImmutableSet<ImmutablePair<String, String>> configuredTables = ImmutableSet.of(configuredTable);

        mockJobArguments();
        when(mockConfigService.getConfiguredTables(CONFIG_KEY)).thenReturn(configuredTables);
        when(mockS3Service.listFilesBeforePeriod(SOURCE_BUCKET, "", configuredTables, parquetFileRegex, Duration.ZERO))
                .thenReturn(Collections.singletonList(new FileLastModifiedDate(FILE_1)));
        when(mockS3Service.copyObjects(any(), eq(SOURCE_BUCKET), eq(DESTINATION_BUCKET), eq(true), any()))
                .thenReturn(Collections.singleton(FILE_1));

        assertThatCallsSystemExit(() -> underTest.run());
    }

    private void mockJobArguments() {
        when(mockJobArguments.getTransferSourceBucket()).thenReturn(SOURCE_BUCKET);
        when(mockJobArguments.getTransferDestinationBucket()).thenReturn(DESTINATION_BUCKET);
        when(mockJobArguments.getConfigKey()).thenReturn(CONFIG_KEY);
    }
}
