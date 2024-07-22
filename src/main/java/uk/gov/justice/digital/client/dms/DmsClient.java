package uk.gov.justice.digital.client.dms;

import com.amazonaws.services.databasemigrationservice.AWSDatabaseMigrationService;
import com.amazonaws.services.databasemigrationservice.model.DescribeReplicationTasksRequest;
import com.amazonaws.services.databasemigrationservice.model.Filter;
import com.amazonaws.services.databasemigrationservice.model.ReplicationTask;
import com.amazonaws.services.databasemigrationservice.model.StopReplicationTaskRequest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.exception.DmsClientException;

import java.util.Date;
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

    public Date getTaskStartTime(String taskId) {
        Optional<ReplicationTask> optionalTask = getTask(taskId);
        if (optionalTask.isPresent()) {
            return Optional.ofNullable(optionalTask.get().getReplicationTaskStartDate())
                    .orElseThrow(() -> new DmsClientException("Start time was null for DMS task with Id: " + taskId));
        } else {
            throw new DmsClientException("Failed to get DMS task with Id: " + taskId);
        }
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

    @NotNull
    private Optional<ReplicationTask> getTask(String taskId) {
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
