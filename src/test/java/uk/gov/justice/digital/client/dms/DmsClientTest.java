package uk.gov.justice.digital.client.dms;

import com.amazonaws.services.databasemigrationservice.AWSDatabaseMigrationService;
import com.amazonaws.services.databasemigrationservice.model.DescribeReplicationTasksResult;
import com.amazonaws.services.databasemigrationservice.model.DescribeReplicationTasksRequest;
import com.amazonaws.services.databasemigrationservice.model.Filter;
import com.amazonaws.services.databasemigrationservice.model.ReplicationTask;
import com.amazonaws.services.databasemigrationservice.model.StopReplicationTaskResult;
import com.amazonaws.services.databasemigrationservice.model.StopReplicationTaskRequest;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
    @Captor
    private ArgumentCaptor<DescribeReplicationTasksRequest> describeReplicationTasksRequestCaptor;
    @Captor
    private ArgumentCaptor<StopReplicationTaskRequest> stopReplicationTaskRequestCaptor;

    private static final String TEST_TASK_ID = "test_task_id";
    private static final int WAIT_INTERVAL_SECONDS = 1;
    private static final int MAX_ATTEMPTS = 1;

    private DmsClient underTest;

    @BeforeEach
    public void setup() {
        reset(mockClientProvider, mockDmsClient, mockDescribeReplicationTasksResult, mockStopReplicationTaskResult);

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
