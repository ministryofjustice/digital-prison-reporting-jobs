package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import io.micronaut.runtime.Micronaut;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
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

    // TODO - remove duplication of context setup
    public static void main(String[] args) {
        logger.info("Job started");
        val context = Micronaut
                .build(args)
                .banner(false)
                .start();
        PicocliRunner.run(DomainRefreshJob.class, context);
    }

    @Override
    public void run() {
        domainService.run();
    }

}
