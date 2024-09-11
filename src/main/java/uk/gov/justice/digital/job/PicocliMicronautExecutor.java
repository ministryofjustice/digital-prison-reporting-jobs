package uk.gov.justice.digital.job;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.job.context.MicronautContext;

import java.time.Clock;

public class PicocliMicronautExecutor {

    private static final Logger logger = LoggerFactory.getLogger(PicocliMicronautExecutor.class);

    /**
     * Executes the provided class as a Picocli Command in the same way as
     * io.micronaut.configuration.picocli.PicocliRunner#execute.
     * We can't use PicocliRunner#execute directly because it does not expose a signature that
     * lets us set the ApplicationContext to our custom MicronautContext.
     * @param cls The class to execute
     * @param args The command line args we will use with Micronaut to instantiate JobArguments
     */
    public static void execute(Class<?> cls, String... args) {
        logger.info("Job starting");
        ApplicationContext applicationContext = MicronautContext.withArgs(args).registerSingleton(Clock.class, Clock.systemUTC());
        CommandLine commandLine = new CommandLine(cls, new MicronautFactory(applicationContext));
        int exitCode = commandLine.execute();
        logger.info("Job finished with exit code {}", exitCode);
        System.exit(exitCode);
    }

    private PicocliMicronautExecutor() {}
}
