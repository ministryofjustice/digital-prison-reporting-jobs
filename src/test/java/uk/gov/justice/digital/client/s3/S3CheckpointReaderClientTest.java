package uk.gov.justice.digital.client.s3;

import com.amazonaws.services.s3.AmazonS3;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.datahub.model.FileLastModifiedDate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3CheckpointReaderClientTest {

    @Mock
    S3ClientProvider mockS3ClientProvider;
    @Mock
    AmazonS3 mockS3Client;
    private final static String CHECKPOINT_BUCKET = "some-bucket";
    private final static String CHECKPOINT_FILE_0 = "v1\n" +
            "{\"path\":\"s3://" + CHECKPOINT_BUCKET + "/source/table0/committed-file-1.parquet\",\"timestamp\":1,\"batchId\":0}\n" +
            "{\"path\":\"s3://" + CHECKPOINT_BUCKET + "/source/table0/committed-file-2.parquet\",\"timestamp\":2,\"batchId\":1}";

    private final static String CHECKPOINT_FILE_1 = "v1\n" +
            "{\"path\":\"s3://" + CHECKPOINT_BUCKET + "/source/table1/committed-file-1.parquet\",\"timestamp\":1,\"batchId\":0}";

    private final static String CHECKPOINT_FILE_2 = "v1\n" +
            "{\"path\":\"s3://" + CHECKPOINT_BUCKET + "/source/table2/committed-file-1.parquet\",\"timestamp\":1,\"batchId\":0}\n" +
            "{\"path\":\"s3://" + CHECKPOINT_BUCKET + "/source/table2/committed-file-2.parquet\",\"timestamp\":2,\"batchId\":1}\n" +
            "{\"path\":\"s3://" + CHECKPOINT_BUCKET + "/source/table2/committed-file-3.parquet\",\"timestamp\":3,\"batchId\":1}";

    private S3CheckpointReaderClient underTest;

    @BeforeEach
    void setup() {
        reset(mockS3ClientProvider, mockS3Client);

        when(mockS3ClientProvider.getClient()).thenReturn(mockS3Client);
        underTest = new S3CheckpointReaderClient(mockS3ClientProvider);
    }

    @Test
    void shouldRetrieveCommittedFilesFromCheckpointFiles() {
        List<FileLastModifiedDate> checkpointFiles = new ArrayList<>();
        checkpointFiles.add(new FileLastModifiedDate("checkpoint-path/2"));
        checkpointFiles.add(new FileLastModifiedDate("checkpoint-path/1"));
        checkpointFiles.add(new FileLastModifiedDate("checkpoint-path/0.compact"));

        when(mockS3Client.getObjectAsString(CHECKPOINT_BUCKET, "checkpoint-path/2")).thenReturn(CHECKPOINT_FILE_2);
        when(mockS3Client.getObjectAsString(CHECKPOINT_BUCKET, "checkpoint-path/1")).thenReturn(CHECKPOINT_FILE_1);
        when(mockS3Client.getObjectAsString(CHECKPOINT_BUCKET, "checkpoint-path/0.compact")).thenReturn(CHECKPOINT_FILE_0);

        Set<String> committedFiles = underTest.getCommittedFiles(CHECKPOINT_BUCKET, checkpointFiles);

        Set<String> expectedCommittedFiles = new HashSet<>();
        expectedCommittedFiles.add("source/table0/committed-file-1.parquet");
        expectedCommittedFiles.add("source/table0/committed-file-2.parquet");
        expectedCommittedFiles.add("source/table1/committed-file-1.parquet");
        expectedCommittedFiles.add("source/table2/committed-file-1.parquet");
        expectedCommittedFiles.add("source/table2/committed-file-2.parquet");
        expectedCommittedFiles.add("source/table2/committed-file-3.parquet");

        assertThat(committedFiles, containsInAnyOrder(expectedCommittedFiles.toArray()));
    }

    @Test
    void shouldIgnoreTempFilesFromCheckpointFilesList() {
        List<FileLastModifiedDate> checkpointFiles = new ArrayList<>();
        checkpointFiles.add(new FileLastModifiedDate("checkpoint-path/2"));
        checkpointFiles.add(new FileLastModifiedDate("checkpoint-path/0.tmp"));

        when(mockS3Client.getObjectAsString(CHECKPOINT_BUCKET, "checkpoint-path/2")).thenReturn(CHECKPOINT_FILE_2);

        Set<String> committedFiles = underTest.getCommittedFiles(CHECKPOINT_BUCKET, checkpointFiles);

        Set<String> expectedCommittedFiles = new HashSet<>();
        expectedCommittedFiles.add("source/table2/committed-file-1.parquet");
        expectedCommittedFiles.add("source/table2/committed-file-2.parquet");
        expectedCommittedFiles.add("source/table2/committed-file-3.parquet");

        assertThat(committedFiles, containsInAnyOrder(expectedCommittedFiles.toArray()));
    }

    @Test
    void shouldIgnoreCheckpointFilesWithNameHavingLowerNaturalOrderThanTheMostRecentCompactFile() {
        List<FileLastModifiedDate> checkpointFiles = new ArrayList<>();
        checkpointFiles.add(new FileLastModifiedDate("checkpoint-path/0")); // this file has name with lower natural order than the compact file and will be ignored
        checkpointFiles.add(new FileLastModifiedDate("checkpoint-path/8"));
        checkpointFiles.add(new FileLastModifiedDate("checkpoint-path/9.compact"));
        checkpointFiles.add(new FileLastModifiedDate("checkpoint-path/10")); // 10 is after 9.compact and should also be processed

        when(mockS3Client.getObjectAsString(CHECKPOINT_BUCKET, "checkpoint-path/10")).thenReturn(CHECKPOINT_FILE_2);
        when(mockS3Client.getObjectAsString(CHECKPOINT_BUCKET, "checkpoint-path/9.compact")).thenReturn(CHECKPOINT_FILE_1);

        Set<String> committedFiles = underTest.getCommittedFiles(CHECKPOINT_BUCKET, checkpointFiles);

        Set<String> expectedCommittedFiles = new HashSet<>();
        expectedCommittedFiles.add("source/table1/committed-file-1.parquet");
        expectedCommittedFiles.add("source/table2/committed-file-1.parquet");
        expectedCommittedFiles.add("source/table2/committed-file-2.parquet");
        expectedCommittedFiles.add("source/table2/committed-file-3.parquet");

        assertThat(committedFiles, containsInAnyOrder(expectedCommittedFiles.toArray()));
    }
}
