package uk.gov.justice.digital.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.service.GlueOrchestrationService;

import javax.inject.Inject;

/**
 * Job stops a running instance of a Glue job.
 */
@CommandLine.Command(name = "StopGlueInstanceJob")
public class StopGlueInstanceJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(StopGlueInstanceJob.class);
    private final GlueOrchestrationService glueOrchestrationService;
    private final JobArguments jobArguments;

    @Inject
    public StopGlueInstanceJob(
            GlueOrchestrationService glueOrchestrationService,
            JobArguments jobArguments
    ) {
        this.glueOrchestrationService = glueOrchestrationService;
        this.jobArguments = jobArguments;
    }

    public static void main(String[] args) {
        PicocliMicronautExecutor.execute(StopGlueInstanceJob.class, args);
    }

    @Override
    public void run() {
        try {
            logger.info("StopGlueInstanceJob running");

            glueOrchestrationService.stopJob(jobArguments.getStopGlueInstanceJobName());

            logger.info("StopGlueInstanceJob finished");
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
    }
}
