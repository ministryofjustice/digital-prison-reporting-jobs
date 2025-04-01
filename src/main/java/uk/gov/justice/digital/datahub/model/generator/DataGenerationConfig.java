package uk.gov.justice.digital.datahub.model.generator;

import java.util.Objects;

public class DataGenerationConfig {
    int batchSize;
    int parallelism;
    long interBatchDelay;
    long runDuration;

    public DataGenerationConfig(int batchSize, int parallelism, long interBatchDelay, long runDuration) {
        this.batchSize = batchSize;
        this.parallelism = parallelism;
        this.interBatchDelay = interBatchDelay;
        this.runDuration = runDuration;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getParallelism() {
        return parallelism;
    }

    public long getInterBatchDelay() {
        return interBatchDelay;
    }

    public long getRunDuration() {
        return runDuration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataGenerationConfig)) return false;
        DataGenerationConfig that = (DataGenerationConfig) o;
        return batchSize == that.batchSize && parallelism == that.parallelism && interBatchDelay == that.interBatchDelay && runDuration == that.runDuration;
    }

    @Override
    public int hashCode() {
        return Objects.hash(batchSize, parallelism, interBatchDelay, runDuration);
    }
}
