package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.AWSGlueException;
import com.amazonaws.services.glue.model.BatchStopJobRunRequest;
import com.amazonaws.services.glue.model.BatchStopJobRunResult;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Connection;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetConnectionResult;
import com.amazonaws.services.glue.model.GetJobRunRequest;
import com.amazonaws.services.glue.model.GetJobRunResult;
import com.amazonaws.services.glue.model.GetJobRunsRequest;
import com.amazonaws.services.glue.model.GetJobRunsResult;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.JobRun;
import com.amazonaws.services.glue.model.StartTriggerRequest;
import com.amazonaws.services.glue.model.StopTriggerRequest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
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
import static uk.gov.justice.digital.client.glue.GlueClient.MAPRED_PARQUET_INPUT_FORMAT;
import static uk.gov.justice.digital.client.glue.GlueClient.SYMLINK_INPUT_FORMAT;
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
    private AWSGlue mockGlueClient;

    @Mock
    private BatchStopJobRunResult mockBatchStopJobRunResult;

    @Mock
    private GetJobRunsResult mockGetJobRunsResult;

    @Mock
    private GetJobRunResult mockGetJobRunResult;

    @Mock
    private GetConnectionResult mockGetConnectionResult;

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

    private GlueClient underTest;

    @BeforeEach
    void setup() {
        sensitiveFields.add(SENSITIVE_FIELD_1);
        sensitiveFields.add(SENSITIVE_FIELD_2);

        reset(mockClientProvider, mockGlueClient, mockBatchStopJobRunResult, mockGetJobRunsResult, mockGetJobRunResult);

        when(mockClientProvider.getClient()).thenReturn(mockGlueClient);
        underTest = new GlueClient(mockClientProvider);
    }

    @Test
    void shouldGetAndReturnConnection() {
        when(mockGlueClient.getConnection(any())).thenReturn(mockGetConnectionResult);
        when(mockGetConnectionResult.getConnection()).thenReturn(mockConnection);

        Connection actualConnection = underTest.getConnection("some-connection-name");
        assertEquals(mockConnection, actualConnection);

        verify(mockGlueClient, times(1)).getConnection(any());
    }

    @Test
    void shouldCreateParquetTable() {
        SourceReference.SensitiveColumns sensitiveColumns = new SourceReference.SensitiveColumns(sensitiveFields);
        underTest.createParquetTable(TEST_DATABASE, TEST_TABLE, TEST_PATH, JSON_DATA_SCHEMA, primaryKeys, sensitiveColumns);

        verify(mockGlueClient, times(1)).createTable(createTableRequestCaptor.capture());

        TableInput tableInput = createTableRequestCaptor.getValue().getTableInput();
        StorageDescriptor storageDescriptor = tableInput.getStorageDescriptor();
        assertThat(storageDescriptor.getLocation(), equalTo(TEST_PATH));
        assertThat(storageDescriptor.getInputFormat(), equalTo(MAPRED_PARQUET_INPUT_FORMAT));
        assertThat(getPrimaryKeys(storageDescriptor), containsInAnyOrder(Collections.singleton(PRIMARY_KEY_FIELD).toArray()));

        Map<String, String> parameters = tableInput.getParameters();
        assertThat(parameters.get("extraction_timestamp_column_name"), is(equalTo(TIMESTAMP.toLowerCase())));
        assertThat(parameters.get("extraction_operation_column_name"), is(equalTo(OPERATION.toLowerCase())));
        assertThat(parameters.get("sensitive_columns"), is(equalTo("['" + SENSITIVE_FIELD_1.toLowerCase() + "','" + SENSITIVE_FIELD_2.toLowerCase() + "']")));
        assertThat(parameters.get("extraction_key"), is(equalTo(TIMESTAMP.toLowerCase() + "," + CHECKPOINT_COL.toLowerCase())));
        assertThat(parameters.get("source_primary_key"), is(equalTo(PRIMARY_KEY_FIELD.toLowerCase())));
    }

    @Test
    void shouldThrowAnExceptionIfAnErrorOccursWhenCreatingParquetTable() {
        SourceReference.SensitiveColumns sensitiveColumns = new SourceReference.SensitiveColumns(sensitiveFields);
        when(mockGlueClient.createTable(any())).thenThrow(new AWSGlueException("failed to create table"));

        assertThrows(
                AWSGlueException.class,
                () -> underTest.createParquetTable(TEST_DATABASE, TEST_TABLE, TEST_PATH, JSON_DATA_SCHEMA, primaryKeys, sensitiveColumns)
        );
    }

    @Test
    void shouldCreateTableWithSymlink() {
        underTest.createTableWithSymlink(TEST_DATABASE, TEST_TABLE, TEST_PATH, JSON_DATA_SCHEMA, primaryKeys);

        verify(mockGlueClient, times(1)).createTable(createTableRequestCaptor.capture());

        StorageDescriptor storageDescriptor = createTableRequestCaptor.getValue().getTableInput().getStorageDescriptor();
        assertThat(storageDescriptor.getLocation(), equalTo(TEST_PATH + "/_symlink_format_manifest"));
        assertThat(storageDescriptor.getInputFormat(), equalTo(SYMLINK_INPUT_FORMAT));
        assertThat(getPrimaryKeys(storageDescriptor), containsInAnyOrder(Collections.singleton(PRIMARY_KEY_FIELD).toArray()));
    }

    @Test
    void shouldThrowAnExceptionIfAnErrorOccursWhenCreatingSymlinkTable() {
        when(mockGlueClient.createTable(any())).thenThrow(new AWSGlueException("failed to create table"));

        assertThrows(
                AWSGlueException.class,
                () -> underTest.createTableWithSymlink(TEST_DATABASE, TEST_TABLE, TEST_PATH, JSON_DATA_SCHEMA, primaryKeys)
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
    void shouldNotFailWhenAnAttemptIsMadeToDeleteTableThatDoesNotExist() {
        when(mockGlueClient.deleteTable(any())).thenThrow(new EntityNotFoundException("failed to delete non-existent table"));

        assertDoesNotThrow(() -> underTest.deleteTable(TEST_DATABASE, TEST_TABLE));
    }

    @Test
    void shouldThrowAnExceptionIfAnyOtherErrorOccursWhenDeletingTable() {
        when(mockGlueClient.deleteTable(any())).thenThrow(new AWSGlueException("failed to delete table"));

        assertThrows(AWSGlueException.class, () -> underTest.deleteTable(TEST_DATABASE, TEST_TABLE));
    }

    @Test
    void stopJobShouldStopRunningJobInstanceWhenThereIsOne() {
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
    void stopJobShouldNotFailWhenThereIsNoRunningJobInstance() {
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
    void stopJobShouldFailWhenUnableToStopRunningInstance() {
        List<JobRun> jobRuns = new ArrayList<>();
        jobRuns.add(createJobRun("RUNNING").withId("running-job-id"));

        when(mockGetJobRunsResult.getJobRuns()).thenReturn(jobRuns);
        when(mockGlueClient.getJobRuns(any())).thenReturn(mockGetJobRunsResult);
        when(mockGlueClient.batchStopJobRun(any())).thenReturn(mockBatchStopJobRunResult);
        when(mockGetJobRunResult.getJobRun()).thenReturn(createJobRun("STOPPING"));
        when(mockGlueClient.getJobRun(any())).thenReturn(mockGetJobRunResult);

        assertThrows(GlueClientException.class, () -> underTest.stopJob(TEST_JOB_NAME, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS));
    }

    @Test
    void shouldActivateTrigger() {
        underTest.activateTrigger(TRIGGER_NAME);

        verify(mockGlueClient, times(1)).startTrigger(startTriggerRequestCaptor.capture());
        assertThat(startTriggerRequestCaptor.getValue().getName(), is(equalTo(TRIGGER_NAME)));
    }

    @Test
    void shouldDeactivateTrigger() {
        underTest.deactivateTrigger(TRIGGER_NAME);

        verify(mockGlueClient, times(1)).stopTrigger(stopTriggerRequestCaptor.capture());
        assertThat(stopTriggerRequestCaptor.getValue().getName(), is(equalTo(TRIGGER_NAME)));
    }

    private static JobRun createJobRun(String RUNNING) {
        return new JobRun().withJobName(TEST_JOB_NAME).withJobRunState(RUNNING);
    }

    @NotNull
    private static Set<String> getPrimaryKeys(StorageDescriptor storageDescriptor) {
        return storageDescriptor
                .getColumns()
                .stream()
                .filter(column -> "primary_key".equals(column.getComment()))
                .map(Column::getName)
                .collect(Collectors.toSet());
    }
}
