package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.exception.DomainServiceException;
import uk.gov.justice.digital.service.DomainService;


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
        PicocliRunner.run(DomainRefreshJob.class);
    }

    @Override
    public void run() {
        try {
            domainService.run();
        } catch (Exception | DomainServiceException e) {
            logger.error("Caught exception during job run", e);
        }

    }

}
