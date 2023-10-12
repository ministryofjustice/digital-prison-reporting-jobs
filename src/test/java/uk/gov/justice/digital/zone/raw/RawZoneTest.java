package uk.gov.justice.digital.zone.raw;


import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ViolationService;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.RAW;
import static uk.gov.justice.digital.test.Fixtures.RAW_PATH;
import static uk.gov.justice.digital.test.Fixtures.TABLE_NAME;
import static uk.gov.justice.digital.test.Fixtures.TABLE_SOURCE;
import static uk.gov.justice.digital.test.ZoneFixtures.createExpectedRawDataset;
import static uk.gov.justice.digital.test.ZoneFixtures.createTestDataset;

@ExtendWith(MockitoExtension.class)
class RawZoneTest extends BaseSparkTest {

    private static final JobArguments jobArguments =
            new JobArguments(Collections.singletonMap(JobArguments.RAW_S3_PATH, RAW_PATH));

    @Mock
    private SourceReference mockSourceReference;

    @Mock

    private DataStorageService mockDataStorageService;
    @Mock
    private ViolationService mockViolationService;

    Dataset<Row> testRecords = createTestDataset(spark);

    private RawZone underTest;

    @BeforeEach
    public void setUp() {
        underTest = new RawZone(
                jobArguments,
                mockDataStorageService,
                mockViolationService
        );
    }

    @Test
    public void processShouldCreateAndAppendDistinctRawRecords() throws DataStorageException {
        val expectedRecords = createExpectedRawDataset(spark);

        val rawPath = createValidatedPath(RAW_PATH, TABLE_SOURCE, TABLE_NAME);

        doNothing()
                .when(mockDataStorageService)
                .appendDistinct(eq(rawPath), refEq(expectedRecords), any());

        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);

        assertIterableEquals(
                expectedRecords.collectAsList(),
                underTest.process(spark, testRecords, mockSourceReference).collectAsList()
        );
    }

    @Test
    public void shouldWriteViolationsWhenDataStorageRetriesExhausted() throws DataStorageException {
        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);

        DataStorageRetriesExhaustedException thrown = new DataStorageRetriesExhaustedException(new Exception("Some problem"));
        doThrow(thrown).when(mockDataStorageService).appendDistinct(any(), any(), any());

        underTest.process(spark, testRecords, mockSourceReference).collect();
        verify(mockViolationService).handleRetriesExhausted(any(), eq(testRecords), eq(TABLE_SOURCE), eq(TABLE_NAME), eq(thrown), eq(RAW));
    }

    @Test
    public void shouldReturnEmptyDataFrameWhenDataStorageRetriesExhausted() throws DataStorageException {
        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);

        DataStorageRetriesExhaustedException thrown = new DataStorageRetriesExhaustedException(new Exception("Some problem"));
        doThrow(thrown).when(mockDataStorageService).appendDistinct(any(), any(), any());

        val resultDf = underTest.process(spark, testRecords, mockSourceReference);
        assertTrue(resultDf.isEmpty());
    }

}
