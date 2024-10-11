package uk.gov.justice.digital.common.retry;

import dev.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoUnit;

import static java.lang.String.format;

public class RetryPolicyBuilder {

    private static final Logger logger = LoggerFactory.getLogger(RetryPolicyBuilder.class);

    /**
     * Builds an instance of a RetryPolicy given a RetryConfig.
     * The retry behaviour is configured in the parameters of the provided RetryConfig
     * <ul>
     * <li>minWaitMillis controls the minimum duration to wait between retries. See {@link dev.failsafe.RetryPolicyBuilder withBackoff}</li>
     * <li>maxWaitMillis controls the maximum duration to wait between retries. See {@link dev.failsafe.RetryPolicyBuilder withBackoff}</li>
     * <li>jitterFactor controls the randomness between retry delays. See {@link dev.failsafe.RetryPolicyBuilder withJitter}</li>
     * <li>maxAttempts controls the number of attempts including the initial one. Retry can be turned off by setting maxAttempts to 1.
     * See {@link dev.failsafe.RetryPolicyBuilder withMaxAttempts}</li>
     * </ul>
     *
     * @param retryConfig the retry config
     * @param exception   the throwable for which the retry should be applied
     * @return the RetryPolicy
     */
    public static RetryPolicy<Void> buildRetryPolicy(RetryConfig retryConfig, Class<? extends Throwable> exception) {
        long minWaitMillis = retryConfig.getMinWaitMillis();
        long maxWaitMillis = retryConfig.getMaxWaitMillis();
        double jitterFactor = retryConfig.getJitterFactor();
        // You can turn off retries by setting max attempts to 1
        int maxAttempts = retryConfig.getMaxAttempts();
        logger.info("Retry Policy Settings: max attempts: {}, min wait: {}ms, max wait: {}ms, jitter factor: {}", maxAttempts, minWaitMillis, maxWaitMillis, jitterFactor);
        dev.failsafe.RetryPolicyBuilder<Void> builder = dev.failsafe.RetryPolicy.builder();
        // Specify the Throwables we will retry
        builder.handle(exception)
                // Exponential backoff
                .withBackoff(minWaitMillis, maxWaitMillis, ChronoUnit.MILLIS)
                .withJitter(jitterFactor)
                .withMaxAttempts(maxAttempts)
                .onFailedAttempt(e -> {
                    Throwable lastException = e.getLastException();
                    int thisAttempt = e.getAttemptCount();
                    String msg = format("Failed attempt %,d.", thisAttempt);
                    logger.debug(msg, lastException);
                })
                .onRetry(e -> {
                    int lastAttempt = e.getAttemptCount();
                    long elapsedTimeTotal = e.getElapsedTime().toMillis();
                    String msg = format("Retrying after attempt %,d. Elapsed time total: %,dms.", lastAttempt, elapsedTimeTotal);
                    logger.debug(msg);
                })
                .onRetriesExceeded(e -> {
                    Throwable lastException = e.getException();
                    int thisAttempt = e.getAttemptCount();
                    long elapsedTimeTotal = e.getElapsedTime().toMillis();
                    String msg = format("Retries exceeded on attempt %,d. Elapsed time total: %,dms.", thisAttempt, elapsedTimeTotal);
                    logger.error(msg, lastException);
                });
        return builder.build();
    }

    private RetryPolicyBuilder() {}

}
