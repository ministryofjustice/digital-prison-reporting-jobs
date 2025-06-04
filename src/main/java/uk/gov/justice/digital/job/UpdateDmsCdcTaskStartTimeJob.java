package uk.gov.justice.digital.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.service.DmsOrchestrationService;

import javax.inject.Inject;

/**
 * Job which updates the start time of a CDC DMS task given the identifier of the Full-Load task.
 */
@CommandLine.Command(name = "UpdateDmsCdcTaskStartTimeJob")
public class UpdateDmsCdcTaskStartTimeJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(UpdateDmsCdcTaskStartTimeJob.class);
    private final DmsOrchestrationService dmsOrchestrationService;
    private final JobArguments jobArguments;

    @Inject
    public UpdateDmsCdcTaskStartTimeJob(
            DmsOrchestrationService dmsOrchestrationService,
            JobArguments jobArguments
    ) {
        this.dmsOrchestrationService = dmsOrchestrationService;
        this.jobArguments = jobArguments;
    }

    public static void main(String[] args) {
        PicocliMicronautExecutor.execute(UpdateDmsCdcTaskStartTimeJob.class, args);
    }

    @Override
    public void run() {
        try {
            logger.info("UpdateDmsCdcTaskStartTimeJob running");

            dmsOrchestrationService.updateCdcTaskStartTime(jobArguments.getDmsTaskId(), jobArguments.getCdcDmsTaskId());

            logger.info("UpdateDmsCdcTaskStartTimeJob finished");
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
            System.exit(1);
        }
    }
}
