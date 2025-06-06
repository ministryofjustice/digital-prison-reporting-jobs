package uk.gov.justice.digital.client.dms;

import com.amazonaws.services.databasemigrationservice.AWSDatabaseMigrationService;
import com.amazonaws.services.databasemigrationservice.model.DescribeReplicationTasksRequest;
import com.amazonaws.services.databasemigrationservice.model.DescribeTableStatisticsRequest;
import com.amazonaws.services.databasemigrationservice.model.DescribeTableStatisticsResult;
import com.amazonaws.services.databasemigrationservice.model.Filter;
import com.amazonaws.services.databasemigrationservice.model.ReplicationTask;
import com.amazonaws.services.databasemigrationservice.model.StopReplicationTaskRequest;
import com.amazonaws.services.databasemigrationservice.model.TableStatistics;
import com.amazonaws.services.databasemigrationservice.model.ModifyReplicationTaskRequest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.exception.DmsClientException;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Singleton
public class DmsClient {

    private static final Logger logger = LoggerFactory.getLogger(DmsClient.class);

    private final AWSDatabaseMigrationService awsDms;

    @Inject
    public DmsClient(DmsClientProvider dmsClientProvider) {
        this.awsDms = dmsClientProvider.getClient();
    }

    public void stopTask(String taskId, int waitIntervalSeconds, int maxAttempts) {
        Optional<ReplicationTask> optionalTask = getTask(taskId);
        optionalTask.ifPresent(task -> {
                    StopReplicationTaskRequest stopReplicationTaskRequest = new StopReplicationTaskRequest()
                            .withReplicationTaskArn(task.getReplicationTaskArn());

                    if (task.getStatus().equalsIgnoreCase("running")) {
                        logger.info("Stopping replication task {}", taskId);
                        awsDms.stopReplicationTask(stopReplicationTaskRequest);

                        try {
                            ensureState(taskId, "stopped", waitIntervalSeconds, maxAttempts);
                        } catch (InterruptedException e) {
                            logger.error("Error while ensuring task {} has stopped", taskId, e);
                            Thread.currentThread().interrupt();
                        }
                    } else {
                        logger.info("Replication task {} is not running", taskId);
                    }
                }
        );
    }

    public void updateCdcTaskStartTime(Date cdcStartTime, String cdcTaskId) {
        Optional<ReplicationTask> optionalTask = getTask(cdcTaskId);

        if (optionalTask.isPresent()) {
            ReplicationTask cdcTask = optionalTask.get();

            logger.info("Modifying replication task");
            val modifyReplicationTaskRequest = new ModifyReplicationTaskRequest()
                    .withReplicationTaskIdentifier(cdcTaskId)
                    .withReplicationTaskArn(cdcTask.getReplicationTaskArn())
                    .withCdcStartTime(cdcStartTime);

            awsDms.modifyReplicationTask(modifyReplicationTaskRequest);
            logger.info("Modified replication task");
        } else {
            throw new DmsClientException("Failed to get DMS task with Id: " + cdcTaskId);
        }
    }

    public Date getTaskStartTime(String taskId) {
        Optional<ReplicationTask> optionalTask = getTask(taskId);
        if (optionalTask.isPresent()) {
            return Optional.ofNullable(optionalTask.get().getReplicationTaskStartDate())
                    .orElseThrow(() -> new DmsClientException("Start time was null for DMS task with Id: " + taskId));
        } else {
            throw new DmsClientException("Failed to get DMS task with Id: " + taskId);
        }
    }

    public List<TableStatistics> getReplicationTaskTableStatistics(String taskId) {
        Optional<ReplicationTask> optionalTask = getTask(taskId);
        ReplicationTask replicationTask = optionalTask.orElseThrow(() ->
                new DmsClientException("Replication task with Id: " + taskId + " not found")
        );
        DescribeTableStatisticsRequest request = new DescribeTableStatisticsRequest();
        request.setReplicationTaskArn(replicationTask.getReplicationTaskArn());
        DescribeTableStatisticsResult response = awsDms.describeTableStatistics(request);
        return response.getTableStatistics();
    }

    private void ensureState(String taskId, String state, int waitIntervalSeconds, int maxAttempts) throws InterruptedException {
        for (int attempts = 0; attempts < maxAttempts; attempts++) {
            logger.info("Ensuring replication task {} is in {} state. Attempt {}", taskId, state, attempts);
            Optional<ReplicationTask> optionalTask = getTask(taskId);
            if (optionalTask.isPresent() && optionalTask.get().getStatus().equalsIgnoreCase(state)) {
                return;
            }

            TimeUnit.SECONDS.sleep(waitIntervalSeconds);
        }

        String errorMessage = String.format("Exhausted attempts waiting for replication task %s to be %s", taskId, state);
        throw new DmsClientException(errorMessage);
    }

    public Optional<ReplicationTask> getTask(String taskId) {
        logger.info("Retrieving replication task {}", taskId);
        val describeReplicationTasksRequest = new DescribeReplicationTasksRequest()
                .withFilters(new Filter().withName("replication-task-id").withValues(taskId))
                .withWithoutSettings(true);

        return awsDms.describeReplicationTasks(describeReplicationTasksRequest)
                .getReplicationTasks()
                .stream()
                .findFirst();
    }
}
