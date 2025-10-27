package uk.gov.justice.digital.service;

import com.amazonaws.services.databasemigrationservice.model.ReplicationTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.dms.DmsClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.DmsClientException;
import uk.gov.justice.digital.exception.DmsOrchestrationServiceException;

import java.util.Date;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DmsOrchestrationServiceTest {

    @Mock
    private JobArguments mockJobArguments;
    @Mock
    private DmsClient mockDmsClient;

    private static final String TEST_TASK_ID = "test_task_id";
    private static final String TEST_CDC_TASK_ID = "test_cdc_task_id";
    private static final int WAIT_INTERVAL_SECONDS = 2;
    private static final int MAX_ATTEMPTS = 10;

    private DmsOrchestrationService underTest;

    @BeforeEach
    void setup() {
        reset(mockJobArguments, mockDmsClient);

        underTest = new DmsOrchestrationService(mockJobArguments, mockDmsClient);
    }

    @Test
    void stopJobShouldStopDmsTaskWithGivenDomainName() {
        when(mockJobArguments.orchestrationWaitIntervalSeconds()).thenReturn(WAIT_INTERVAL_SECONDS);
        when(mockJobArguments.orchestrationMaxAttempts()).thenReturn(MAX_ATTEMPTS);

        underTest.stopTask(TEST_TASK_ID);

        verify(mockDmsClient, times(1)).stopTask(TEST_TASK_ID, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS);
    }

    @Test
    void stopJobShouldFailWhenDmsClientThrowsAnException() {
        when(mockJobArguments.orchestrationWaitIntervalSeconds()).thenReturn(WAIT_INTERVAL_SECONDS);
        when(mockJobArguments.orchestrationMaxAttempts()).thenReturn(MAX_ATTEMPTS);

        doThrow(new DmsClientException("Client error")).when(mockDmsClient)
                .stopTask(TEST_TASK_ID, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS);

        assertThrows(DmsClientException.class, () -> underTest.stopTask(TEST_TASK_ID));
    }

    @Test
    void getTaskStartTimeShouldReturnTheDmsTskStartTime() {
        underTest.getTaskStartTime(TEST_TASK_ID);

        verify(mockDmsClient, times(1)).getTaskStartTime(TEST_TASK_ID);
    }

    @Test
    void getTaskStartTimeShouldFailWhenDmsClientThrowsAnException() {
        doThrow(new DmsClientException("Client error")).when(mockDmsClient).getTaskStartTime(TEST_TASK_ID);

        assertThrows(DmsClientException.class, () -> underTest.getTaskStartTime(TEST_TASK_ID));
    }

    @Test
    void updateCdcTaskStartTimeShouldSetTheStartTimeOfCdcDmsTask() {
        Date startTime = new Date();

        when(mockDmsClient.getTask(TEST_TASK_ID)).thenReturn(Optional.of(new ReplicationTask().withReplicationTaskStartDate(startTime)));
        doNothing().when(mockDmsClient).updateCdcTaskStartTime(startTime, TEST_CDC_TASK_ID);

        assertDoesNotThrow(() -> underTest.updateCdcTaskStartTime(TEST_TASK_ID, TEST_CDC_TASK_ID));
    }

    @Test
    void updateCdcTaskStartTimeShouldThrowExceptionWhenUnableToFindFullLoadTask() {
        when(mockDmsClient.getTask(TEST_TASK_ID)).thenReturn(Optional.empty());

        assertThrows(DmsOrchestrationServiceException.class, () -> underTest.updateCdcTaskStartTime(TEST_TASK_ID, TEST_CDC_TASK_ID));

        verifyNoMoreInteractions(mockDmsClient);
    }
}
