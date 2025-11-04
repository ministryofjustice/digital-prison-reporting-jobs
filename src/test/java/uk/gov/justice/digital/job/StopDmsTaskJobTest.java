package uk.gov.justice.digital.job;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.SparkTestBase;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.DmsClientException;
import uk.gov.justice.digital.service.DmsOrchestrationService;

import static com.ginsberg.junit.exit.assertions.SystemExitAssertion.assertThatCallsSystemExit;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StopDmsTaskJobTest extends SparkTestBase {

    @Mock
    DmsOrchestrationService mockDmsOrchestrationService;
    @Mock
    JobArguments mockJobArguments;

    private static final String TEST_TASK_ID = "test_task_id";

    private StopDmsTaskJob underTest;

    @BeforeEach
    void setup() {
        reset(mockDmsOrchestrationService, mockJobArguments);

        underTest = new StopDmsTaskJob(mockDmsOrchestrationService, mockJobArguments);
    }

    @Test
    void shouldStopDmsTaskWithGivenDomain() {
        when(mockJobArguments.getDmsTaskId()).thenReturn(TEST_TASK_ID);

        underTest.run();

        verify(mockDmsOrchestrationService, times(1)).stopTask(TEST_TASK_ID);
    }

    @Test
    void shouldFailWhenAnExceptionOccursInService() {
        when(mockJobArguments.getDmsTaskId()).thenReturn(TEST_TASK_ID);
        doThrow(new DmsClientException("error")).when(mockDmsOrchestrationService).stopTask(any());

        assertThatCallsSystemExit(() -> underTest.run()).withExitCode(1);
    }
}
