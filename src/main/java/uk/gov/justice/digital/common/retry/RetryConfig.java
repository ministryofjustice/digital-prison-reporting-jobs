package uk.gov.justice.digital.common.retry;

import lombok.Data;
import uk.gov.justice.digital.config.JobArguments;

@Data
public class RetryConfig {
    private final long minWaitMillis;
    private final long maxWaitMillis;
    private final double jitterFactor;
    private final int maxAttempts;

    public RetryConfig(JobArguments jobArguments) {
        this.minWaitMillis = jobArguments.getDataStorageRetryMinWaitMillis();
        this.maxWaitMillis = jobArguments.getDataStorageRetryMaxWaitMillis();
        this.jitterFactor = jobArguments.getDataStorageRetryJitterFactor();
        this.maxAttempts = jobArguments.getDataStorageRetryMaxAttempts();
    }
}
