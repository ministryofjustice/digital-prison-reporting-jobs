package uk.gov.justice.digital.service;

import com.amazonaws.AmazonClientException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3CheckpointReaderClient;
import uk.gov.justice.digital.client.s3.S3ObjectClient;
import uk.gov.justice.digital.config.JobArguments;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verifyNoInteractions;
import static uk.gov.justice.digital.common.RegexPatterns.matchAllFiles;
import static uk.gov.justice.digital.test.Fixtures.fixedClock;

@ExtendWith(MockitoExtension.class)
class CheckpointReaderServiceTest {

    @Mock
    private JobArguments mockJobArguments;
    @Mock
    private S3ObjectClient mockS3Client;
    @Mock
    private S3CheckpointReaderClient mockCheckpointReaderClient;

    private final String CHECKPOINT_BUCKET = "some-bucket";
    private final String CHECKPOINT_FOLDER = "checkpoint-dir/";
    private final String CHECKPOINT_LOCATION = "s3://" + CHECKPOINT_BUCKET + "/" + CHECKPOINT_FOLDER;

    private CheckpointReaderService underTest;

    @BeforeEach
    public void setup() {
        reset(mockJobArguments, mockS3Client);

        underTest = new CheckpointReaderService(mockS3Client, mockCheckpointReaderClient, mockJobArguments, fixedClock);
    }

    @Test
    void getCommittedFilesForTableShouldReturnEmptyListWhenThereAreNoCheckpointFiles() {
        ImmutablePair<String, String> configuredTable = ImmutablePair.of("schema_1", "table_1");
        when(mockJobArguments.getCheckpointLocation()).thenReturn(CHECKPOINT_LOCATION);
        when(mockS3Client.getObjectsOlderThan(any(), any(), any(), any(), any())).thenReturn(Collections.emptyList());

        Set<String> committedFiles = underTest.getCommittedFilesForTable(configuredTable);

        assertThat(committedFiles, is(empty()));
    }

    @Test
    void getCommittedFilesForTableShouldReturnCommittedFiles() {
        ImmutablePair<String, String> configuredTable = ImmutablePair.of("schema_1", "table_1");
        String checkpointFolder = CHECKPOINT_FOLDER + "DataHubCdcJob/Datahub_CDC_" + configuredTable.left + "." + configuredTable.right + "/sources/0/";

        List<String> checkpointFiles = new ArrayList<>();
        checkpointFiles.add(checkpointFolder + "checkpoint-file-2");
        checkpointFiles.add(checkpointFolder + "checkpoint-file-3");
        checkpointFiles.add(checkpointFolder + "checkpoint-file-0.compact");
        checkpointFiles.add(checkpointFolder + "checkpoint-file-1");

        Set<String> expectedCommittedFiles = new HashSet<>();
        expectedCommittedFiles.add("committed-file-1");
        expectedCommittedFiles.add("committed-file-3");
        expectedCommittedFiles.add("committed-file-2");

        when(mockJobArguments.getCheckpointLocation()).thenReturn(CHECKPOINT_LOCATION);
        when(mockS3Client.getObjectsOlderThan(CHECKPOINT_BUCKET, checkpointFolder, matchAllFiles, Duration.ZERO, fixedClock)).thenReturn(checkpointFiles);
        when(mockCheckpointReaderClient.getCommittedFiles(CHECKPOINT_BUCKET, checkpointFiles)).thenReturn(expectedCommittedFiles);

        Set<String> committedFiles = underTest.getCommittedFilesForTable(configuredTable);

        assertThat(committedFiles, containsInAnyOrder(expectedCommittedFiles.toArray()));
    }

    @ParameterizedTest
    @CsvSource({
            "non-s3-URI-string",
            "s3://",
            "s3://" + CHECKPOINT_BUCKET,
            "s3://" + CHECKPOINT_BUCKET + "/",
            "s3:///checkpoint-folder"
    })
    void getCommittedFilesForTableShouldThrowExceptionWhenUnableToResolveCheckpointBucketOrFolder(String input) {
        ImmutablePair<String, String> configuredTable = ImmutablePair.of("schema_1", "table_1");
        when(mockJobArguments.getCheckpointLocation()).thenReturn(input);

        assertThrows(IllegalArgumentException.class, () -> underTest.getCommittedFilesForTable(configuredTable));

        verifyNoInteractions(mockS3Client, mockCheckpointReaderClient);
    }

    @Test
    void getCommittedFilesForTableShouldPropagateExceptionsThrownByS3Client() {
        ImmutablePair<String, String> configuredTable = ImmutablePair.of("schema_1", "table_1");
        when(mockJobArguments.getCheckpointLocation()).thenReturn(CHECKPOINT_LOCATION);
        doThrow(new AmazonClientException("s3-client-error")).when(mockS3Client).getObjectsOlderThan(any(), any(), any(), any(), any());

        assertThrows(AmazonClientException.class, () -> underTest.getCommittedFilesForTable(configuredTable));
    }

    @Test
    void getCommittedFilesForTableShouldPropagateExceptionsThrownByTheCheckpointReaderClientClient() {
        ImmutablePair<String, String> configuredTable = ImmutablePair.of("schema_1", "table_1");
        when(mockJobArguments.getCheckpointLocation()).thenReturn(CHECKPOINT_LOCATION);
        when(mockS3Client.getObjectsOlderThan(any(), any(), any(), any(), any())).thenReturn(Collections.emptyList());
        doThrow(new AmazonClientException("checkpoint-client-error")).when(mockCheckpointReaderClient).getCommittedFiles(any(), any());

        assertThrows(AmazonClientException.class, () -> underTest.getCommittedFilesForTable(configuredTable));
    }
}
