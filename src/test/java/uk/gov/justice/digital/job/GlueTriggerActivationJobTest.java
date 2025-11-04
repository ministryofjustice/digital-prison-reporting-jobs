package uk.gov.justice.digital.job;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.SparkTestBase;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.GlueClientException;
import uk.gov.justice.digital.service.GlueOrchestrationService;

import static com.ginsberg.junit.exit.assertions.SystemExitAssertion.assertThatCallsSystemExit;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.any;

@ExtendWith(MockitoExtension.class)
class GlueTriggerActivationJobTest extends SparkTestBase {

    @Mock
    GlueOrchestrationService mockGlueOrchestrationService;
    @Mock
    JobArguments mockJobArguments;

    private static final String TRIGGER_NAME = "some-trigger-name";

    private GlueTriggerActivationJob underTest;

    @BeforeEach
    void setup() {
        reset(mockGlueOrchestrationService, mockJobArguments);

        underTest = new GlueTriggerActivationJob(mockGlueOrchestrationService, mockJobArguments);
    }

    @Test
    void shouldActivateGlueTriggerWithGivenNameWhenActivateTriggerArgumentIsTrue() {
        when(mockJobArguments.getGlueTriggerName()).thenReturn(TRIGGER_NAME);
        when(mockJobArguments.activateGlueTrigger()).thenReturn(true);

        underTest.run();

        verify(mockGlueOrchestrationService, times(1)).activateTrigger(TRIGGER_NAME);
    }

    @Test
    void shouldDeactivateGlueTriggerWithGivenNameWhenActivateTriggerArgumentIsFalse() {
        when(mockJobArguments.getGlueTriggerName()).thenReturn(TRIGGER_NAME);
        when(mockJobArguments.activateGlueTrigger()).thenReturn(false);

        underTest.run();

        verify(mockGlueOrchestrationService, times(1)).deactivateTrigger(TRIGGER_NAME);
    }

    @Test
    void shouldFailWhenAnExceptionOccursInServiceWhileActivatingTrigger() throws Exception {
        when(mockJobArguments.getGlueTriggerName()).thenReturn(TRIGGER_NAME);
        when(mockJobArguments.activateGlueTrigger()).thenReturn(true);
        doThrow(new GlueClientException("error")).when(mockGlueOrchestrationService).activateTrigger(any());

        assertThatCallsSystemExit(() -> underTest.run()).withExitCode(1);
    }

    @Test
    void shouldFailWhenAnExceptionOccursInServiceWhileDeactivatingTrigger() throws Exception {
        when(mockJobArguments.getGlueTriggerName()).thenReturn(TRIGGER_NAME);
        when(mockJobArguments.activateGlueTrigger()).thenReturn(false);
        doThrow(new GlueClientException("error")).when(mockGlueOrchestrationService).deactivateTrigger(any());

        assertThatCallsSystemExit(() -> underTest.run()).withExitCode(1);
    }
}
