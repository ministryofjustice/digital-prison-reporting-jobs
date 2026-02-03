package uk.gov.justice.digital.service.shutdownhooks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public final class ShutdownHooks {

    private static final Logger logger = LoggerFactory.getLogger(ShutdownHooks.class);

    /**
     * Wraps a provided shutdownHook in a runnable that attempts to execute it within a
     * specified timeout. If the shutdown hook does not complete within the timeout,
     * the thread is abandoned so that JVM shutdown can continue.
     *
     * @return a Runnable that wraps the provided shutdown hook within a timeout mechanism.
     */
    public static Runnable withTimeout(String name, Duration timeout, Runnable shutdownHook) {
        return () -> {
            Thread bestEffort = new Thread(shutdownHook);
            bestEffort.setDaemon(true);
            bestEffort.start();
            try {
                bestEffort.join(timeout.toMillis());
            } catch (InterruptedException e) {
                logger.warn("Shutdown hook '{}' interrupted", name, e);
                Thread.currentThread().interrupt();
            }
        };
    }
}
