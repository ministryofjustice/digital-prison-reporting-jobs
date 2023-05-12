package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.exception.DomainServiceException;
import uk.gov.justice.digital.service.DomainService;
import java.util.Arrays;


/**
 * Job that refreshes domains so that the data in the consumer-facing systems is correctly formatted and up-to-date.
 *  It reads domains from DomainRegistry (a DynamoDB table) and does one of the following:
 *   1. Refresh whole domain
 *   2. Refresh one table in a domain
 */
@Singleton
@CommandLine.Command(name = "DomainRefreshJob")
public class DomainRefreshJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DomainRefreshJob.class);

    private final DomainService domainService;
    private final JobParameters jobParameters;

    private static String[] jobArguments;


    @Inject
    public DomainRefreshJob(DomainService domainService, JobParameters jobParameters) {
        this.domainService = domainService;
        this.jobParameters = jobParameters;
    }

    public static void main(String[] args) {
        logger.info("Job started");
        logger.info("Arguments :" + Arrays.toString(args));
        jobArguments = args;

        PicocliRunner.run(DomainRefreshJob.class);
    }

    @Override
    public void run() {
        try {
            jobParameters.parse(jobArguments);
            domainService.run();
        } catch (Exception | DomainServiceException e) {
            logger.error("Caught exception during job run", e);
        }

    }

}
