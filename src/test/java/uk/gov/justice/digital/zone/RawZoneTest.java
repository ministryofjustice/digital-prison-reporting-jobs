package uk.gov.justice.digital.zone;


import lombok.val;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.zone.Fixtures.*;

@ExtendWith(MockitoExtension.class)
class RawZoneTest {

    private static final JobArguments jobArguments =
            new JobArguments(Collections.singletonMap(JobArguments.RAW_S3_PATH, RAW_PATH));

    @Mock
    private Dataset<Row> mockDataset;

    @Mock
    private SourceReference mockSourceReference;

    @Mock
    private DataStorageService mockDataStorageService;

    @Mock
    private SparkSession mockSparkSession;

    @Mock
    private SourceReferenceService mockSourceReferenceService;

    @Test
    public void processShouldCompleteSuccessfully() throws DataStorageException {
        val rawPath = createValidatedPath(RAW_PATH, TABLE_SOURCE, TABLE_NAME, TABLE_OPERATION);

        when(mockSourceReferenceService.getSourceReference(TABLE_SOURCE, TABLE_NAME))
                .thenReturn(Optional.of(mockSourceReference));

        doNothing().when(mockDataStorageService).appendDistinct(rawPath, mockDataset, RawZone.PRIMARY_KEY_NAME);

        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);

        when(mockDataset.count()).thenReturn(10L);
        when(mockDataset.select(Mockito.<Column>any())).thenReturn(mockDataset);

        val underTest = new RawZone(
                jobArguments,
                mockDataStorageService,
                mockSourceReferenceService
        );

        assertEquals(mockDataset, underTest.process(mockSparkSession, mockDataset, dataMigrationEventRow));
    }

    @Test
    public void processShouldSkipProcessingGivenInvalidOperation() throws DataStorageException {
        when(mockDataset.count()).thenReturn(10L);

        val underTest = new RawZone(
                jobArguments,
                mockDataStorageService,
                mockSourceReferenceService
        );

        assertEquals(mockDataset, underTest.process(mockSparkSession, mockDataset, dataMigrationEventRowWithInvalidOperation));
    }
}
