package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.config.JobParameters;

import java.util.Arrays;

@Singleton
@CommandLine.Command(name = "FakeJob")
public class FakeJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataHubJob.class);

    private final JobParameters jobParameters;

    @Inject
    public FakeJob(JobParameters jobParameters) {
        logger.info("Setting job parameters");
        this.jobParameters = jobParameters;
        logger.info("Job parameters set");
    }

    public static void main(String[] args) {
        logger.info("Job started");
        val foo = Arrays.asList(args);
        logger.info("Args dump: {}", Strings.join(foo, ' '));
        PicocliRunner.run(FakeJob.class, args);
    }

    @Override
    public void run() {
        logger.info("Run called");
    }

}
