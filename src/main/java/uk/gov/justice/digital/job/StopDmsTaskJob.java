package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.service.DmsOrchestrationService;

import javax.inject.Inject;

/**
 * Job stops a DMS task.
 */
@CommandLine.Command(name = "StopDmsTaskJob")
public class StopDmsTaskJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(StopDmsTaskJob.class);
    private final DmsOrchestrationService dmsOrchestrationService;
    private final JobArguments jobArguments;

    @Inject
    public StopDmsTaskJob(
            DmsOrchestrationService dmsOrchestrationService,
            JobArguments jobArguments
    ) {
        this.dmsOrchestrationService = dmsOrchestrationService;
        this.jobArguments = jobArguments;
    }

    public static void main(String[] args) {
        logger.info("Job starting");
        PicocliRunner.run(StopDmsTaskJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        try {
            logger.info("StopDmsTaskJob running");

            dmsOrchestrationService.stopTask(jobArguments.getDmsTaskId());

            logger.info("StopDmsTaskJob finished");
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
    }
}
