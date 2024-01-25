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
        int waitIntervalSeconds = jobArguments.glueOrchestrationWaitIntervalSeconds();
        int maxAttempts = jobArguments.glueOrchestrationMaxAttempts();

        logger.info("Stopping Glue job {}", jobName);
        glueClient.stopJob(jobName, waitIntervalSeconds, maxAttempts);
        logger.info("Stopped Glue job {}", jobName);
    }
}
