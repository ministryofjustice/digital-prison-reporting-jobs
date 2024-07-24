package uk.gov.justice.digital.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.dms.DmsClient;
import uk.gov.justice.digital.config.JobArguments;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Date;

@Singleton
public class DmsOrchestrationService {

    private static final Logger logger = LoggerFactory.getLogger(DmsOrchestrationService.class);

    private final JobArguments jobArguments;

    private final DmsClient dmsClient;

    @Inject
    public DmsOrchestrationService(JobArguments jobArguments, DmsClient dmsClient) {
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

    public Date getTaskStartTime(String taskId) {
        return dmsClient.getTaskStartTime(taskId);
    }
}
