package uk.gov.justice.digital.job;

import com.ginsberg.junit.exit.ExpectSystemExitWithStatus;
import org.junit.jupiter.api.Test;

class PicocliMicronautExecutorTest {

    @Test
    @ExpectSystemExitWithStatus(1)
    void aJobThatFailsToInstantiateShouldSystemExitWithNonZeroCode() {
        // The job with no arguments should return a status code of 1
        String[] emptyArgs = new String[0];
        PicocliMicronautExecutor.execute(DataReconciliationJob.class, emptyArgs);
    }

}