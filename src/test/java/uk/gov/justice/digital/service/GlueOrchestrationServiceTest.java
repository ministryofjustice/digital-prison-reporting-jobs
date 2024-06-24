package uk.gov.justice.digital.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.glue.GlueClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.GlueClientException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GlueOrchestrationServiceTest {

    @Mock
    private JobArguments mockJobArguments;
    @Mock
    private GlueClient mockGlueClient;

    private static final String TEST_JOB_NAME = "test_glue_job";
    private static final int WAIT_INTERVAL_SECONDS = 2;
    private static final int MAX_ATTEMPTS = 10;
    private static final String TRIGGER_NAME = "some-trigger-name";

    private GlueOrchestrationService underTest;

    @BeforeEach
    public void setup() {
        reset(mockJobArguments, mockGlueClient);

        underTest = new GlueOrchestrationService(mockJobArguments, mockGlueClient);
    }

    @Test
    void stopJobShouldStopGlueJobWithGivenName() {
        when(mockJobArguments.orchestrationWaitIntervalSeconds()).thenReturn(WAIT_INTERVAL_SECONDS);
        when(mockJobArguments.orchestrationMaxAttempts()).thenReturn(MAX_ATTEMPTS);

        underTest.stopJob(TEST_JOB_NAME);

        verify(mockGlueClient, times(1)).stopJob(TEST_JOB_NAME, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS);
    }

    @Test
    void stopJobShouldFailWhenGlueClientThrowsAnException() {
        when(mockJobArguments.orchestrationWaitIntervalSeconds()).thenReturn(WAIT_INTERVAL_SECONDS);
        when(mockJobArguments.orchestrationMaxAttempts()).thenReturn(MAX_ATTEMPTS);

        doThrow(new GlueClientException("Client error")).when(mockGlueClient)
                .stopJob(TEST_JOB_NAME, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS);

        assertThrows(GlueClientException.class, () -> underTest.stopJob(TEST_JOB_NAME));
    }

    @Test
    void shouldActivateGlueTrigger() {
        underTest.activateTrigger(TRIGGER_NAME);

        verify(mockGlueClient, times(1)).activateTrigger(TRIGGER_NAME);
    }

    @Test
    void activateTriggerShouldFailWhenGlueClientThrowsAnException() {
        doThrow(new GlueClientException("Client error")).when(mockGlueClient).activateTrigger(any());

        assertThrows(GlueClientException.class, () -> underTest.activateTrigger(TRIGGER_NAME));
    }

    @Test
    void shouldDeactivateGlueTrigger() {
        underTest.deactivateTrigger(TRIGGER_NAME);

        verify(mockGlueClient, times(1)).deactivateTrigger(TRIGGER_NAME);
    }

    @Test
    void deactivateTriggerShouldFailWhenGlueClientThrowsAnException() {
        doThrow(new GlueClientException("Client error")).when(mockGlueClient).deactivateTrigger(any());

        assertThrows(GlueClientException.class, () -> underTest.deactivateTrigger(TRIGGER_NAME));
    }
}
