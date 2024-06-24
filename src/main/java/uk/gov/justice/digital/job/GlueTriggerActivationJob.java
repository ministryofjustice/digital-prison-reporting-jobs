package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.service.GlueOrchestrationService;

import javax.inject.Inject;

/**
 * Job that activates/deactivates a Glue trigger.
 */
@CommandLine.Command(name = "GlueTriggerActivationJob")
public class GlueTriggerActivationJob implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(GlueTriggerActivationJob.class);
    private final GlueOrchestrationService glueOrchestrationService;
    private final JobArguments jobArguments;

    @Inject
    public GlueTriggerActivationJob(GlueOrchestrationService glueOrchestrationService, JobArguments jobArguments) {
        this.glueOrchestrationService = glueOrchestrationService;
        this.jobArguments = jobArguments;
    }

    public static void main(String[] args) {
        logger.info("Job starting");
        PicocliRunner.run(GlueTriggerActivationJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        try {
            logger.info("GlueTriggerActivationJob running");
            String triggerName = jobArguments.getGlueTriggerName();
            boolean activateTrigger = jobArguments.activateGlueTrigger();

            if (activateTrigger) {
                glueOrchestrationService.activateTrigger(triggerName);
            } else {
                glueOrchestrationService.deactivateTrigger(triggerName);
            }

            logger.info("GlueTriggerActivationJob finished");
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
    }
}
