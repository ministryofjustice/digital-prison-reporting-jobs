package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.PicocliRunner;
import io.micronaut.runtime.Micronaut;
import jakarta.inject.Singleton;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@Singleton
@CommandLine.Command(name = "FakeJob")
public class FakeJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataHubJob.class);

    public static void main(String[] args) {
        logger.info("Job started");
        val context = Micronaut
            .build(args)
            .banner(false)
            .start();
        PicocliRunner.run(FakeJob.class, context);
    }

    @Override
    public void run() {
        logger.info("Run called");
    }

}
