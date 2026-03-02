package uk.gov.justice.digital.client.glue;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;

import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.BatchStopJobRunResponse;
import software.amazon.awssdk.services.glue.model.GetJobRunsResponse;
import software.amazon.awssdk.services.glue.model.GetJobRunResponse;
import software.amazon.awssdk.services.glue.model.GetConnectionResponse;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.BatchStopJobRunRequest;
import software.amazon.awssdk.services.glue.model.GetJobRunsRequest;
import software.amazon.awssdk.services.glue.model.GetJobRunRequest;
import software.amazon.awssdk.services.glue.model.StartTriggerRequest;
import software.amazon.awssdk.services.glue.model.StopTriggerRequest;
import software.amazon.awssdk.services.glue.model.GetConnectionRequest;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.JobRunState;
import software.amazon.awssdk.services.glue.model.JobRun;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.Connection;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.GlueClientException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.times;
import static uk.gov.justice.digital.client.glue.DefaultGlueClient.MAPRED_PARQUET_INPUT_FORMAT;
import static uk.gov.justice.digital.client.glue.DefaultGlueClient.SYMLINK_INPUT_FORMAT;
import static uk.gov.justice.digital.common.CommonDataFields.TIMESTAMP;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.CHECKPOINT_COL;
import static uk.gov.justice.digital.test.Fixtures.PRIMARY_KEY_FIELD;
import static uk.gov.justice.digital.test.Fixtures.SENSITIVE_FIELD_1;
import static uk.gov.justice.digital.test.Fixtures.SENSITIVE_FIELD_2;
import static uk.gov.justice.digital.test.Fixtures.JSON_DATA_SCHEMA;

@ExtendWith(MockitoExtension.class)
class GlueClientTest {

    @Mock
    private GlueClientProvider mockClientProvider;

    @Mock
    private GlueClient mockGlueClient;

    @Mock
    private BatchStopJobRunResponse mockBatchStopJobRunResponse;

    @Mock
    private GetJobRunsResponse mockGetJobRunsResponse;

    @Mock
    private GetJobRunResponse mockGetJobRunResponse;

    @Mock
    private GetConnectionResponse mockGetConnectionResponse;

    @Mock
    private Connection mockConnection;

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

    @Captor
    ArgumentCaptor<StartTriggerRequest> startTriggerRequestCaptor;

    @Captor
    ArgumentCaptor<StopTriggerRequest> stopTriggerRequestCaptor;

    private static final String TEST_DATABASE = "test-database";
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_JOB_NAME = "test-job-name";
    private static final String TEST_PATH = "/some/data/path";

    private static final int WAIT_INTERVAL_SECONDS = 1;

    private static final int MAX_ATTEMPTS = 1;
    private static final String TRIGGER_NAME = "some-trigger-name";

    private static final SourceReference.PrimaryKey primaryKeys = new SourceReference.PrimaryKey(PRIMARY_KEY_FIELD);

    private static final Collection<String> sensitiveFields = new ArrayList<>();

    private DefaultGlueClient underTest;

    @BeforeEach
    void setup() {
        sensitiveFields.add(SENSITIVE_FIELD_1);
        sensitiveFields.add(SENSITIVE_FIELD_2);

        reset(mockClientProvider, mockGlueClient, mockBatchStopJobRunResponse, mockGetJobRunsResponse, mockGetJobRunResponse);

        when(mockClientProvider.getClient()).thenReturn(mockGlueClient);
        underTest = new DefaultGlueClient(mockClientProvider);
    }

    @Test
    void shouldGetAndReturnConnection() {
        when(mockGlueClient.getConnection(any(GetConnectionRequest.class))).thenReturn(mockGetConnectionResponse);
        when(mockGetConnectionResponse.connection()).thenReturn(mockConnection);

        Connection actualConnection = underTest.getConnection("some-connection-name");
        assertEquals(mockConnection, actualConnection);

        verify(mockGlueClient, times(1)).getConnection(any(GetConnectionRequest.class));
    }

    @Test
    void shouldCreateParquetTable() {
        SourceReference.SensitiveColumns sensitiveColumns = new SourceReference.SensitiveColumns(sensitiveFields);
        underTest.createParquetTable(TEST_DATABASE, TEST_TABLE, TEST_PATH, JSON_DATA_SCHEMA, primaryKeys, sensitiveColumns);

        verify(mockGlueClient, times(1)).createTable(createTableRequestCaptor.capture());

        TableInput tableInput = createTableRequestCaptor.getValue().tableInput();
        StorageDescriptor storageDescriptor = tableInput.storageDescriptor();
        assertThat(storageDescriptor.location(), equalTo(TEST_PATH));
        assertThat(storageDescriptor.inputFormat(), equalTo(MAPRED_PARQUET_INPUT_FORMAT));
        assertThat(getPrimaryKeys(storageDescriptor), containsInAnyOrder(Collections.singleton(PRIMARY_KEY_FIELD).toArray()));

        Map<String, String> parameters = tableInput.parameters();
        assertThat(parameters.get("extraction_timestamp_column_name"), is(equalTo(TIMESTAMP.toLowerCase())));
        assertThat(parameters.get("extraction_operation_column_name"), is(equalTo(OPERATION.toLowerCase())));
        assertThat(parameters.get("sensitive_columns"), is(equalTo("['" + SENSITIVE_FIELD_1.toLowerCase() + "','" + SENSITIVE_FIELD_2.toLowerCase() + "']")));
        assertThat(parameters.get("extraction_key"), is(equalTo(TIMESTAMP.toLowerCase() + "," + CHECKPOINT_COL.toLowerCase())));
        assertThat(parameters.get("source_primary_key"), is(equalTo(PRIMARY_KEY_FIELD.toLowerCase())));
    }

    @Test
    void shouldThrowAnExceptionIfAnErrorOccursWhenCreatingParquetTable() {
        SourceReference.SensitiveColumns sensitiveColumns = new SourceReference.SensitiveColumns(sensitiveFields);
        when(mockGlueClient.createTable(any(CreateTableRequest.class))).thenThrow(GlueException.builder().message("failed to create table").build());

        assertThrows(
                GlueException.class,
                () -> underTest.createParquetTable(TEST_DATABASE, TEST_TABLE, TEST_PATH, JSON_DATA_SCHEMA, primaryKeys, sensitiveColumns)
        );
    }

    @Test
    void shouldCreateTableWithSymlink() {
        underTest.createTableWithSymlink(TEST_DATABASE, TEST_TABLE, TEST_PATH, JSON_DATA_SCHEMA, primaryKeys);

        verify(mockGlueClient, times(1)).createTable(createTableRequestCaptor.capture());

        StorageDescriptor storageDescriptor = createTableRequestCaptor.getValue().tableInput().storageDescriptor();
        assertThat(storageDescriptor.location(), equalTo(TEST_PATH + "/_symlink_format_manifest"));
        assertThat(storageDescriptor.inputFormat(), equalTo(SYMLINK_INPUT_FORMAT));
        assertThat(getPrimaryKeys(storageDescriptor), containsInAnyOrder(Collections.singleton(PRIMARY_KEY_FIELD).toArray()));
    }

    @Test
    void shouldThrowAnExceptionIfAnErrorOccursWhenCreatingSymlinkTable() {
        when(mockGlueClient.createTable(any(CreateTableRequest.class))).thenThrow(GlueException.builder().message("failed to create table").build());

        assertThrows(
                GlueException.class,
                () -> underTest.createTableWithSymlink(TEST_DATABASE, TEST_TABLE, TEST_PATH, JSON_DATA_SCHEMA, primaryKeys)
        );
    }

    @Test
    void shouldDeleteTable() {
        underTest.deleteTable(TEST_DATABASE, TEST_TABLE);

        verify(mockGlueClient, times(1)).deleteTable(deleteTableRequestCaptor.capture());

        DeleteTableRequest deleteTableRequest = deleteTableRequestCaptor.getValue();
        assertThat(deleteTableRequest.databaseName(), equalTo(TEST_DATABASE));
        assertThat(deleteTableRequest.name(), equalTo(TEST_TABLE));
    }

    @Test
    void shouldNotFailWhenAnAttemptIsMadeToDeleteTableThatDoesNotExist() {
        when(mockGlueClient.deleteTable(any(DeleteTableRequest.class))).thenThrow(EntityNotFoundException.builder().message("failed to delete non-existent table").build());

        assertDoesNotThrow(() -> underTest.deleteTable(TEST_DATABASE, TEST_TABLE));
    }

    @Test
    void shouldThrowAnExceptionIfAnyOtherErrorOccursWhenDeletingTable() {
        when(mockGlueClient.deleteTable(any(DeleteTableRequest.class))).thenThrow(GlueException.builder().message("failed to delete table").build());

        assertThrows(GlueException.class, () -> underTest.deleteTable(TEST_DATABASE, TEST_TABLE));
    }

    @Test
    void stopJobShouldStopRunningJobInstanceWhenThereIsOne() {
        List<JobRun> jobRuns = new ArrayList<>();
        jobRuns.add(createJobRun(JobRunState.STOPPED));
        jobRuns.add(createJobRun(JobRunState.STOPPING));
        jobRuns.add(createJobRun(JobRunState.STARTING));
        jobRuns.add(createJobRun(JobRunState.SUCCEEDED));
        jobRuns.add(createJobRun(JobRunState.FAILED));
        jobRuns.add(JobRun.builder().jobName(TEST_JOB_NAME).jobRunState(JobRunState.RUNNING).id("running-job-id").build());
        jobRuns.add(createJobRun(JobRunState.TIMEOUT));
        jobRuns.add(createJobRun(JobRunState.ERROR));
        jobRuns.add(createJobRun(JobRunState.WAITING));

        when(mockGetJobRunsResponse.jobRuns()).thenReturn(jobRuns);
        when(mockGlueClient.getJobRuns(getJobRunsRequestCaptor.capture())).thenReturn(mockGetJobRunsResponse);
        when(mockGlueClient.batchStopJobRun(stopJobRunRequestCaptor.capture())).thenReturn(mockBatchStopJobRunResponse);
        when(mockGetJobRunResponse.jobRun()).thenReturn(createJobRun(JobRunState.STOPPED));
        when(mockGlueClient.getJobRun(getJobRunRequestCaptor.capture())).thenReturn(mockGetJobRunResponse);

        underTest.stopJob(TEST_JOB_NAME, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS);

        GetJobRunsRequest getJobRunsRequestCaptorValue = getJobRunsRequestCaptor.getValue();
        assertThat(getJobRunsRequestCaptorValue.jobName(), is(equalTo(TEST_JOB_NAME)));
        assertThat(getJobRunsRequestCaptorValue.maxResults(), is(equalTo(200)));
        assertThat(stopJobRunRequestCaptor.getValue().jobName(), is(equalTo(TEST_JOB_NAME)));
        assertThat(getJobRunRequestCaptor.getValue().jobName(), is(equalTo(TEST_JOB_NAME)));
    }

    @Test
    void stopJobShouldNotFailWhenThereIsNoRunningJobInstance() {
        List<JobRun> jobRuns = Collections.emptyList();

        when(mockGetJobRunsResponse.jobRuns()).thenReturn(jobRuns);
        when(mockGlueClient.getJobRuns(getJobRunsRequestCaptor.capture())).thenReturn(mockGetJobRunsResponse);

        underTest.stopJob(TEST_JOB_NAME, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS);

        GetJobRunsRequest getJobRunsRequestCaptorValue = getJobRunsRequestCaptor.getValue();
        assertThat(getJobRunsRequestCaptorValue.jobName(), is(equalTo(TEST_JOB_NAME)));
        assertThat(getJobRunsRequestCaptorValue.maxResults(), is(equalTo(200)));
        verifyNoInteractions(mockGetJobRunResponse);
    }

    @Test
    void stopJobShouldFailWhenUnableToStopRunningInstance() {
        List<JobRun> jobRuns = new ArrayList<>();
        jobRuns.add(JobRun.builder().jobName(TEST_JOB_NAME).jobRunState(JobRunState.RUNNING).id("running-job-id").build());

        when(mockGetJobRunsResponse.jobRuns()).thenReturn(jobRuns);
        when(mockGlueClient.getJobRuns(any(GetJobRunsRequest.class))).thenReturn(mockGetJobRunsResponse);
        when(mockGlueClient.batchStopJobRun(any(BatchStopJobRunRequest.class))).thenReturn(mockBatchStopJobRunResponse);
        when(mockGetJobRunResponse.jobRun()).thenReturn(createJobRun(JobRunState.STOPPING));
        when(mockGlueClient.getJobRun(any(GetJobRunRequest.class))).thenReturn(mockGetJobRunResponse);

        assertThrows(GlueClientException.class, () -> underTest.stopJob(TEST_JOB_NAME, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS));
    }

    @Test
    void shouldActivateTrigger() {
        underTest.activateTrigger(TRIGGER_NAME);

        verify(mockGlueClient, times(1)).startTrigger(startTriggerRequestCaptor.capture());
        assertThat(startTriggerRequestCaptor.getValue().name(), is(equalTo(TRIGGER_NAME)));
    }

    @Test
    void shouldDeactivateTrigger() {
        underTest.deactivateTrigger(TRIGGER_NAME);

        verify(mockGlueClient, times(1)).stopTrigger(stopTriggerRequestCaptor.capture());
        assertThat(stopTriggerRequestCaptor.getValue().name(), is(equalTo(TRIGGER_NAME)));
    }

    private static JobRun createJobRun(JobRunState state) {
        return JobRun.builder().jobName(TEST_JOB_NAME).jobRunState(state).build();
    }

    @NotNull
    private static Set<String> getPrimaryKeys(StorageDescriptor storageDescriptor) {
        return storageDescriptor
                .columns()
                .stream()
                .filter(column -> "primary_key".equals(column.comment()))
                .map(Column::name)
                .collect(Collectors.toSet());
    }
}
