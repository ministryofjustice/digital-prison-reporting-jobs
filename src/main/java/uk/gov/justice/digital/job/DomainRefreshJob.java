package uk.gov.justice.digital.job;

import com.amazonaws.services.glue.AWSGlue;
import io.micronaut.configuration.picocli.PicocliRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.service.DomainService;
import javax.inject.Inject;
import javax.inject.Singleton;

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


    @Inject
    public DomainRefreshJob(DomainService domainService) {
        this.domainService = domainService;
    }

    public static void main(String[] args) {
        logger.info("Job started");
        PicocliRunner.run(DomainRefreshJob.class, args);
    }

    @Override
    public void run() {
        try {
            domainService.run();
        } catch (Exception e) {
            logger.error("Caught exception during job run", e);
        }

    }

}
