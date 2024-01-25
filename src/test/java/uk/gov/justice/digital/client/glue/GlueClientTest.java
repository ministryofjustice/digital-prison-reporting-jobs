package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.exception.GlueClientException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.client.glue.GlueClient.*;
import static uk.gov.justice.digital.test.Fixtures.JSON_DATA_SCHEMA;

@ExtendWith(MockitoExtension.class)
class GlueClientTest {

    @Mock
    private GlueClientProvider mockClientProvider;

    @Mock
    private AWSGlue mockGlueClient;

    @Mock
    private BatchStopJobRunResult mockBatchStopJobRunResult;

    @Mock
    private GetJobRunsResult mockGetJobRunsResult;

    @Mock
    private GetJobRunResult mockGetJobRunResult;

    @Captor
    ArgumentCaptor<CreateTableRequest> createTableRequestCaptor;

    @Captor
    ArgumentCaptor<DeleteTableRequest> deleteTableRequestCaptor;

    @Captor
    ArgumentCaptor<BatchStopJobRunRequest> stopJobRunRequestCaptor;

    @Captor
    ArgumentCaptor<GetJobRunsRequest> getJobRunsRequestCaptor;

    @Captor
    ArgumentCaptor<GetJobRunRequest> getJobRunRequestCaptor;

    private static final String TEST_DATABASE = "test-database";
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_JOB_NAME = "test-job-name";
    private static final String TEST_PATH = "/some/data/path";

    private static final int WAIT_INTERVAL_SECONDS = 1;

    private static final int MAX_ATTEMPTS = 1;

    private GlueClient underTest;

    @BeforeEach
    public void setup() {
        reset(mockClientProvider, mockGlueClient, mockBatchStopJobRunResult, mockGetJobRunsResult, mockGetJobRunResult);

        when(mockClientProvider.getClient()).thenReturn(mockGlueClient);
        underTest = new GlueClient(mockClientProvider);
    }

    @Test
    void shouldCreateParquetTable() {
        underTest.createParquetTable(TEST_DATABASE, TEST_TABLE, TEST_PATH, JSON_DATA_SCHEMA);

        verify(mockGlueClient, times(1)).createTable(createTableRequestCaptor.capture());

        StorageDescriptor storageDescriptor = createTableRequestCaptor.getValue().getTableInput().getStorageDescriptor();
        assertThat(storageDescriptor.getLocation(), equalTo(TEST_PATH));
        assertThat(storageDescriptor.getInputFormat(), equalTo(MAPRED_PARQUET_INPUT_FORMAT));
    }

    @Test
    public void shouldThrowAnExceptionIfAnErrorOccursWhenCreatingParquetTable() {
        when(mockGlueClient.createTable(any())).thenThrow(new AWSGlueException("failed to create table"));

        assertThrows(
                AWSGlueException.class,
                () -> underTest.createParquetTable(TEST_DATABASE, TEST_TABLE, TEST_PATH, JSON_DATA_SCHEMA)
        );
    }

    @Test
    void shouldCreateTableWithSymlink() {
        underTest.createTableWithSymlink(TEST_DATABASE, TEST_TABLE, TEST_PATH, JSON_DATA_SCHEMA);

        verify(mockGlueClient, times(1)).createTable(createTableRequestCaptor.capture());

        StorageDescriptor storageDescriptor = createTableRequestCaptor.getValue().getTableInput().getStorageDescriptor();
        assertThat(storageDescriptor.getLocation(), equalTo(TEST_PATH + "/_symlink_format_manifest"));
        assertThat(storageDescriptor.getInputFormat(), equalTo(SYMLINK_INPUT_FORMAT));
    }

    @Test
    public void shouldThrowAnExceptionIfAnErrorOccursWhenCreatingSymlinkTable() {
        when(mockGlueClient.createTable(any())).thenThrow(new AWSGlueException("failed to create table"));

        assertThrows(
                AWSGlueException.class,
                () -> underTest.createTableWithSymlink(TEST_DATABASE, TEST_TABLE, TEST_PATH, JSON_DATA_SCHEMA)
        );
    }

    @Test
    void shouldDeleteTable() {
        underTest.deleteTable(TEST_DATABASE, TEST_TABLE);

        verify(mockGlueClient, times(1)).deleteTable(deleteTableRequestCaptor.capture());

        DeleteTableRequest deleteTableRequest = deleteTableRequestCaptor.getValue();
        assertThat(deleteTableRequest.getDatabaseName(), equalTo(TEST_DATABASE));
        assertThat(deleteTableRequest.getName(), equalTo(TEST_TABLE));
    }

    @Test
    public void shouldNotFailWhenAnAttemptIsMadeToDeleteTableThatDoesNotExist() {
        when(mockGlueClient.deleteTable(any())).thenThrow(new EntityNotFoundException("failed to delete non-existent table"));

        assertDoesNotThrow(() -> underTest.deleteTable(TEST_DATABASE, TEST_TABLE));
    }

    @Test
    public void shouldThrowAnExceptionIfAnyOtherErrorOccursWhenDeletingTable() {
        when(mockGlueClient.deleteTable(any())).thenThrow(new AWSGlueException("failed to delete table"));

        assertThrows(AWSGlueException.class, () -> underTest.deleteTable(TEST_DATABASE, TEST_TABLE));
    }

    @Test
    public void stopJobShouldStopRunningJobInstanceWhenThereIsOne() {
        List<JobRun> jobRuns = new ArrayList<>();
        jobRuns.add(createJobRun("STOPPED"));
        jobRuns.add(createJobRun("STOPPING"));
        jobRuns.add(createJobRun("STARTING"));
        jobRuns.add(createJobRun("SUCCEEDED"));
        jobRuns.add(createJobRun("FAILED"));
        jobRuns.add(createJobRun("RUNNING").withId("running-job-id"));
        jobRuns.add(createJobRun("TIMEOUT"));
        jobRuns.add(createJobRun("ERROR"));
        jobRuns.add(createJobRun("WAITING"));

        when(mockGetJobRunsResult.getJobRuns()).thenReturn(jobRuns);
        when(mockGlueClient.getJobRuns(getJobRunsRequestCaptor.capture())).thenReturn(mockGetJobRunsResult);
        when(mockGlueClient.batchStopJobRun(stopJobRunRequestCaptor.capture())).thenReturn(mockBatchStopJobRunResult);
        when(mockGetJobRunResult.getJobRun()).thenReturn(createJobRun("STOPPED"));
        when(mockGlueClient.getJobRun(getJobRunRequestCaptor.capture())).thenReturn(mockGetJobRunResult);

        underTest.stopJob(TEST_JOB_NAME, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS);

        GetJobRunsRequest getJobRunsRequestCaptorValue = getJobRunsRequestCaptor.getValue();
        assertThat(getJobRunsRequestCaptorValue.getJobName(), is(equalTo(TEST_JOB_NAME)));
        assertThat(getJobRunsRequestCaptorValue.getMaxResults(), is(equalTo(200)));
        assertThat(stopJobRunRequestCaptor.getValue().getJobName(), is(equalTo(TEST_JOB_NAME)));
        assertThat(getJobRunRequestCaptor.getValue().getJobName(), is(equalTo(TEST_JOB_NAME)));
    }

    @Test
    public void stopJobShouldNotFailWhenThereIsNoRunningJobInstance() {
        List<JobRun> jobRuns = Collections.emptyList();

        when(mockGetJobRunsResult.getJobRuns()).thenReturn(jobRuns);
        when(mockGlueClient.getJobRuns(getJobRunsRequestCaptor.capture())).thenReturn(mockGetJobRunsResult);

        underTest.stopJob(TEST_JOB_NAME, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS);

        GetJobRunsRequest getJobRunsRequestCaptorValue = getJobRunsRequestCaptor.getValue();
        assertThat(getJobRunsRequestCaptorValue.getJobName(), is(equalTo(TEST_JOB_NAME)));
        assertThat(getJobRunsRequestCaptorValue.getMaxResults(), is(equalTo(200)));
        verifyNoInteractions(mockGetJobRunResult);
    }

    @Test
    public void stopJobShouldFailWhenUnableToStopRunningInstance() {
        List<JobRun> jobRuns = new ArrayList<>();
        jobRuns.add(createJobRun("RUNNING").withId("running-job-id"));

        when(mockGetJobRunsResult.getJobRuns()).thenReturn(jobRuns);
        when(mockGlueClient.getJobRuns(any())).thenReturn(mockGetJobRunsResult);
        when(mockGlueClient.batchStopJobRun(any())).thenReturn(mockBatchStopJobRunResult);
        when(mockGetJobRunResult.getJobRun()).thenReturn(createJobRun("STOPPING"));
        when(mockGlueClient.getJobRun(any())).thenReturn(mockGetJobRunResult);

        assertThrows(GlueClientException.class, () -> underTest.stopJob(TEST_JOB_NAME, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS));
    }

    private static JobRun createJobRun(String RUNNING) {
        return new JobRun().withJobName(TEST_JOB_NAME).withJobRunState(RUNNING);
    }
}