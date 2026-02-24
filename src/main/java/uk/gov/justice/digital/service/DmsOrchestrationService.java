package uk.gov.justice.digital.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.databasemigration.model.ReplicationTask;
import uk.gov.justice.digital.client.dms.DefaultDmsClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.DmsOrchestrationServiceException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Instant;
import java.util.Optional;

@Singleton
public class DmsOrchestrationService {

    private static final Logger logger = LoggerFactory.getLogger(DmsOrchestrationService.class);

    private final JobArguments jobArguments;

    private final DefaultDmsClient dmsClient;

    @Inject
    public DmsOrchestrationService(JobArguments jobArguments, DefaultDmsClient dmsClient) {
        this.jobArguments = jobArguments;
        this.dmsClient = dmsClient;
    }

    public void stopTask(String taskId) {
        int waitIntervalSeconds = jobArguments.orchestrationWaitIntervalSeconds();
        int maxAttempts = jobArguments.orchestrationMaxAttempts();

        logger.info("Stopping DMS task {}", taskId);
        dmsClient.stopTask(taskId, waitIntervalSeconds, maxAttempts);
        logger.info("Stopped DMS task {}", taskId);
    }

    public void updateCdcTaskStartTime(String fullLoadTaskId, String cdcTaskId) {
        Optional<ReplicationTask> optionalTask = dmsClient.getTask(fullLoadTaskId);

        if (optionalTask.isPresent()) {
            ReplicationTask fullLoadTask = optionalTask.get();
            Instant fullLoadStartTime = fullLoadTask.replicationTaskStartDate();
            logger.info("Updating start time for CDC replication task");
            dmsClient.updateCdcTaskStartTime(fullLoadStartTime, cdcTaskId);
            logger.info("Updated start time for replication task {}", cdcTaskId);
        } else {
            String errorMessage = String.format("Unable to find replication task with id %s", cdcTaskId);
            throw new DmsOrchestrationServiceException(errorMessage);
        }
    }

    public Instant getTaskStartTime(String taskId) {
        return dmsClient.getTaskStartTime(taskId);
    }
}
