package uk.gov.justice.digital.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.glue.GlueClient;
import uk.gov.justice.digital.config.JobArguments;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class GlueOrchestrationService {

    private static final Logger logger = LoggerFactory.getLogger(GlueOrchestrationService.class);

    private final JobArguments jobArguments;

    private final GlueClient glueClient;

    @Inject
    public GlueOrchestrationService(JobArguments jobArguments, GlueClient glueClient) {
        this.jobArguments = jobArguments;
        this.glueClient = glueClient;
    }

    public void stopJob(String jobName) {
        int waitIntervalSeconds = jobArguments.orchestrationWaitIntervalSeconds();
        int maxAttempts = jobArguments.orchestrationMaxAttempts();

        logger.info("Stopping Glue job {}", jobName);
        glueClient.stopJob(jobName, waitIntervalSeconds, maxAttempts);
        logger.info("Stopped Glue job {}", jobName);
    }

    public void activateTrigger(String triggerName) {
        logger.info("Activating Glue trigger {}", triggerName);
        glueClient.activateTrigger(triggerName);
        logger.info("Activated Glue trigger {}", triggerName);
    }

    public void deactivateTrigger(String triggerName) {
        logger.info("Deactivating Glue trigger {}", triggerName);
        glueClient.deactivateTrigger(triggerName);
        logger.info("Deactivated Glue trigger {}", triggerName);
    }
}
