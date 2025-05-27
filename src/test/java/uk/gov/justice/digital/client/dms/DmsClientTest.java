package uk.gov.justice.digital.client.dms;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.databasemigrationservice.AWSDatabaseMigrationService;
import com.amazonaws.services.databasemigrationservice.model.DescribeReplicationTasksRequest;
import com.amazonaws.services.databasemigrationservice.model.DescribeReplicationTasksResult;
import com.amazonaws.services.databasemigrationservice.model.DescribeTableStatisticsRequest;
import com.amazonaws.services.databasemigrationservice.model.DescribeTableStatisticsResult;
import com.amazonaws.services.databasemigrationservice.model.ModifyReplicationTaskRequest;
import com.amazonaws.services.databasemigrationservice.model.ModifyReplicationTaskResult;
import com.amazonaws.services.databasemigrationservice.model.Filter;
import com.amazonaws.services.databasemigrationservice.model.ReplicationTask;
import com.amazonaws.services.databasemigrationservice.model.StopReplicationTaskRequest;
import com.amazonaws.services.databasemigrationservice.model.StopReplicationTaskResult;
import com.amazonaws.services.databasemigrationservice.model.TableStatistics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.exception.DmsClientException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;

@ExtendWith(MockitoExtension.class)
class DmsClientTest {

    @Mock
    private DmsClientProvider mockClientProvider;
    @Mock
    private AWSDatabaseMigrationService mockDmsClient;
    @Mock
    private DescribeReplicationTasksResult mockDescribeReplicationTasksResult;
    @Mock
    private StopReplicationTaskResult mockStopReplicationTaskResult;
    @Mock
    private DescribeTableStatisticsResult mockDescribeTableStatisticsResult;
    @Captor
    private ArgumentCaptor<DescribeReplicationTasksRequest> describeReplicationTasksRequestCaptor;
    @Captor
    private ArgumentCaptor<StopReplicationTaskRequest> stopReplicationTaskRequestCaptor;
    @Captor
    private ArgumentCaptor<DescribeTableStatisticsRequest> describeTableStatisticsRequestCaptor;
    @Captor
    private ArgumentCaptor<ModifyReplicationTaskRequest> modifyReplicationTaskRequestCaptor;

    private static final String TEST_TASK_ID = "test_task_id";
    private static final String TASK_TASK_ARN = "test_replication_task_arn";
    private static final int WAIT_INTERVAL_SECONDS = 1;
    private static final int MAX_ATTEMPTS = 1;

    private DmsClient underTest;

    @BeforeEach
    public void setup() {
        reset(mockClientProvider, mockDmsClient, mockDescribeReplicationTasksResult, mockStopReplicationTaskResult, mockDescribeTableStatisticsResult);

        when(mockClientProvider.getClient()).thenReturn(mockDmsClient);
        underTest = new DmsClient(mockClientProvider);
    }

    @SuppressWarnings("unchecked")
    @Test
    void stopTaskShouldStopRunningDmsTaskWhenThereIsOne() {
        List<ReplicationTask> runningTasks = new ArrayList<>();
        runningTasks.add(createReplicationTask("running").withReplicationTaskArn("replication-task-arn"));

        List<ReplicationTask> stoppedTasks = new ArrayList<>();
        stoppedTasks.add(createReplicationTask("stopped").withReplicationTaskArn("replication-task-arn"));

        when(mockDescribeReplicationTasksResult.getReplicationTasks()).thenReturn(runningTasks, stoppedTasks);
        when(mockDmsClient.describeReplicationTasks(describeReplicationTasksRequestCaptor.capture()))
                .thenReturn(mockDescribeReplicationTasksResult);
        when(mockDmsClient.stopReplicationTask(stopReplicationTaskRequestCaptor.capture()))
                .thenReturn(mockStopReplicationTaskResult);

        underTest.stopTask(TEST_TASK_ID, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS);

        StopReplicationTaskRequest stopReplicationTaskRequest = stopReplicationTaskRequestCaptor.getValue();
        assertThat(stopReplicationTaskRequest.getReplicationTaskArn(), equalTo("replication-task-arn"));
        verifyDescribeReplicationTasksRequestParams(describeReplicationTasksRequestCaptor.getValue());
    }

    @Test
    void stopTaskShouldNotFailWhenTaskIsAlreadyStopped() {
        List<ReplicationTask> stoppedTasks = new ArrayList<>();
        stoppedTasks.add(createReplicationTask("stopped").withReplicationTaskArn("replication-task-arn"));

        when(mockDescribeReplicationTasksResult.getReplicationTasks()).thenReturn(stoppedTasks);
        when(mockDmsClient.describeReplicationTasks(describeReplicationTasksRequestCaptor.capture()))
                .thenReturn(mockDescribeReplicationTasksResult);

        underTest.stopTask(TEST_TASK_ID, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS);

        verifyDescribeReplicationTasksRequestParams(describeReplicationTasksRequestCaptor.getValue());
        verifyNoMoreInteractions(mockDmsClient);
    }

    @Test
    void stopTaskShouldNotFailWhenThereIsNoRunningReplicationTask() {
        List<ReplicationTask> tasks = Collections.emptyList();

        when(mockDescribeReplicationTasksResult.getReplicationTasks()).thenReturn(tasks);
        when(mockDmsClient.describeReplicationTasks(describeReplicationTasksRequestCaptor.capture())).thenReturn(mockDescribeReplicationTasksResult);

        underTest.stopTask(TEST_TASK_ID, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS);

        verifyDescribeReplicationTasksRequestParams(describeReplicationTasksRequestCaptor.getValue());
        verifyNoMoreInteractions(mockDescribeReplicationTasksResult);
    }

    @Test
    void getTaskStartTimeShouldReturnTheStartTimeOfReplicationTask() {
        Date taskStartTime = new Date();
        List<ReplicationTask> tasks = new ArrayList<>();
        tasks.add(createReplicationTask("stopped").withReplicationTaskStartDate(taskStartTime));

        when(mockDescribeReplicationTasksResult.getReplicationTasks()).thenReturn(tasks);
        when(mockDmsClient.describeReplicationTasks(describeReplicationTasksRequestCaptor.capture()))
                .thenReturn(mockDescribeReplicationTasksResult);

        Date actualTaskStartTime = underTest.getTaskStartTime(TEST_TASK_ID);

        assertThat(actualTaskStartTime, equalTo(taskStartTime));
        verifyDescribeReplicationTasksRequestParams(describeReplicationTasksRequestCaptor.getValue());
        verifyNoMoreInteractions(mockDescribeReplicationTasksResult);
    }

    @Test
    void getTaskStartTimeShouldThrowAnExceptionWhenReplicationTaskStartTimeIsNull() {
        List<ReplicationTask> tasks = new ArrayList<>();
        tasks.add(createReplicationTask("stopped").withReplicationTaskStartDate(null));

        when(mockDescribeReplicationTasksResult.getReplicationTasks()).thenReturn(tasks);
        when(mockDmsClient.describeReplicationTasks(describeReplicationTasksRequestCaptor.capture()))
                .thenReturn(mockDescribeReplicationTasksResult);

        assertThrows(DmsClientException.class, () -> underTest.getTaskStartTime(TEST_TASK_ID));
        verifyDescribeReplicationTasksRequestParams(describeReplicationTasksRequestCaptor.getValue());
        verifyNoMoreInteractions(mockDescribeReplicationTasksResult);
    }

    @Test
    void getTaskStartTimeShouldThrowAnExceptionWhenNoReplicationTaskIsFound() {
        List<ReplicationTask> tasks = Collections.emptyList();

        when(mockDescribeReplicationTasksResult.getReplicationTasks()).thenReturn(tasks);
        when(mockDmsClient.describeReplicationTasks(describeReplicationTasksRequestCaptor.capture()))
                .thenReturn(mockDescribeReplicationTasksResult);

        assertThrows(DmsClientException.class, () -> underTest.getTaskStartTime(TEST_TASK_ID));
        verifyDescribeReplicationTasksRequestParams(describeReplicationTasksRequestCaptor.getValue());
        verifyNoMoreInteractions(mockDescribeReplicationTasksResult);
    }

    @Test
    void getReplicationTaskTableStatisticsShouldReturnListOfTableStatistics() {
        List<ReplicationTask> tasks = new ArrayList<>();
        tasks.add(createReplicationTask("running")
                .withReplicationTaskIdentifier(TEST_TASK_ID)
                .withReplicationTaskArn("replication-task-arn"));
        when(mockDmsClient.describeReplicationTasks(describeReplicationTasksRequestCaptor.capture()))
                .thenReturn(mockDescribeReplicationTasksResult);
        when(mockDescribeReplicationTasksResult.getReplicationTasks()).thenReturn(tasks);


        when(mockDmsClient.describeTableStatistics(describeTableStatisticsRequestCaptor.capture()))
                .thenReturn(mockDescribeTableStatisticsResult);

        List<TableStatistics> expectedTableStatistics = Collections.singletonList(
                new TableStatistics()
                        .withAppliedDeletes(10L)
                        .withDeletes(10L)
        );
        when(mockDescribeTableStatisticsResult.getTableStatistics()).thenReturn(expectedTableStatistics);

        List<TableStatistics> actualTableStatistics = underTest.getReplicationTaskTableStatistics(TEST_TASK_ID);
        assertEquals(expectedTableStatistics, actualTableStatistics);

        verifyDescribeReplicationTasksRequestParams(describeReplicationTasksRequestCaptor.getValue());
        assertEquals("replication-task-arn", describeTableStatisticsRequestCaptor.getValue().getReplicationTaskArn());
        verifyNoMoreInteractions(mockDescribeReplicationTasksResult);
    }

    @Test
    void getReplicationTaskTableStatisticsShouldThrowWhenNoReplicationTaskIsFound() {
        List<ReplicationTask> tasks = Collections.emptyList();

        when(mockDmsClient.describeReplicationTasks(any())).thenReturn(mockDescribeReplicationTasksResult);
        when(mockDescribeReplicationTasksResult.getReplicationTasks()).thenReturn(tasks);

        assertThrows(DmsClientException.class, () -> underTest.getReplicationTaskTableStatistics(TEST_TASK_ID));
    }

    @Test
    void updateCdcTaskStartTimeShouldSetTheStartTimeOfCdcTask() {
        Date cdcStartTime = new Date();

        ReplicationTask cdcReplicationTask = new ReplicationTask().withReplicationTaskArn(TASK_TASK_ARN);
        DescribeReplicationTasksResult describeReplicationTasksResult = new DescribeReplicationTasksResult()
                .withReplicationTasks(Collections.singletonList(cdcReplicationTask));

        when(mockDmsClient.describeReplicationTasks(any())).thenReturn(describeReplicationTasksResult);
        when(mockDmsClient.modifyReplicationTask(modifyReplicationTaskRequestCaptor.capture()))
                .thenReturn(new ModifyReplicationTaskResult());

        underTest.updateCdcTaskStartTime(cdcStartTime, TEST_TASK_ID);

        ModifyReplicationTaskRequest modifyReplicationTaskRequest = modifyReplicationTaskRequestCaptor.getValue();
        assertThat(modifyReplicationTaskRequest.getReplicationTaskArn(), equalTo(TASK_TASK_ARN));
        assertThat(modifyReplicationTaskRequest.getReplicationTaskIdentifier(), equalTo(TEST_TASK_ID));
        assertThat(modifyReplicationTaskRequest.getCdcStartTime(), equalTo(cdcStartTime));
    }

    @Test
    void updateCdcTaskStartTimeShouldThrowExceptionWhenNoReplicationTaskIsFound() {
        Date cdcStartTime = new Date();
        List<ReplicationTask> tasks = Collections.emptyList();

        DescribeReplicationTasksResult describeReplicationTasksResult = new DescribeReplicationTasksResult()
                .withReplicationTasks(tasks);

        when(mockDmsClient.describeReplicationTasks(any())).thenReturn(describeReplicationTasksResult);

        assertThrows(DmsClientException.class, () -> underTest.updateCdcTaskStartTime(cdcStartTime, TEST_TASK_ID));
        verifyNoMoreInteractions(mockDmsClient);
    }

    @Test
    void updateCdcTaskStartTimeShouldThrowExceptionWhenDescribingReplicationTasksFails() {
        Date cdcStartTime = new Date();

        doThrow(new AmazonServiceException("Client exception")).when(mockDmsClient).describeReplicationTasks(any());

        assertThrows(AmazonServiceException.class, () -> underTest.updateCdcTaskStartTime(cdcStartTime, TEST_TASK_ID));
        verifyNoMoreInteractions(mockDmsClient);
    }

    @Test
    void updateCdcTaskStartTimeShouldThrowExceptionWhenModifyingReplicationTaskFails() {
        Date cdcStartTime = new Date();

        ReplicationTask cdcReplicationTask = new ReplicationTask().withReplicationTaskArn(TASK_TASK_ARN);
        DescribeReplicationTasksResult describeReplicationTasksResult = new DescribeReplicationTasksResult()
                .withReplicationTasks(Collections.singletonList(cdcReplicationTask));

        when(mockDmsClient.describeReplicationTasks(any())).thenReturn(describeReplicationTasksResult);
        doThrow(new AmazonServiceException("Client exception")).when(mockDmsClient).modifyReplicationTask(any());

        assertThrows(AmazonServiceException.class, () -> underTest.updateCdcTaskStartTime(cdcStartTime, TEST_TASK_ID));
    }

    private static void verifyDescribeReplicationTasksRequestParams(DescribeReplicationTasksRequest describeReplicationTasksRequest) {
        assertThat(
                new HashSet<>(describeReplicationTasksRequest.getFilters()),
                containsInAnyOrder(new Filter().withName("replication-task-id").withValues(TEST_TASK_ID))
        );
        assertTrue(describeReplicationTasksRequest.getWithoutSettings());
    }

    private static ReplicationTask createReplicationTask(String state) {
        return new ReplicationTask().withStatus(state);
    }
}
