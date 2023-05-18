package uk.gov.justice.digital.zone;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.config.JobArguments.CURATED_S3_PATH;
import static uk.gov.justice.digital.zone.Fixtures.*;

@ExtendWith(MockitoExtension.class)
class CuratedZoneTest {

    private static final JobArguments jobArguments =
            new JobArguments(Collections.singletonMap(CURATED_S3_PATH, CURATED_PATH));

    @Mock
    private Dataset<Row> mockedDataSet;

    @Mock
    private SourceReference mockSourceReference;

    @Mock
    private DataStorageService mockDataStorageService;

    @Mock
    private SparkSession mockSparkSession;

    @Test
    public void shouldProcessCuratedZone() throws DataStorageException {
        try (val service = mockStatic(SourceReferenceService.class)) {
            val curatedPath = createValidatedPath(CURATED_PATH, TABLE_SOURCE, TABLE_NAME);

            service.when(() -> SourceReferenceService.getSourceReference(TABLE_SOURCE, TABLE_NAME))
                    .thenReturn(Optional.of(mockSourceReference));

            doNothing().when(mockDataStorageService).append(curatedPath, mockedDataSet);

            when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
            when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);

            when(mockedDataSet.count()).thenReturn(10L);

            val underTest = new CuratedZone(jobArguments, mockDataStorageService);

            assertNotNull(underTest.process(mockSparkSession, mockedDataSet, dataMigrationEventRow));
        }
    }
}
