package uk.gov.justice.digital.zone;


import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.test.Fixtures.*;
import static uk.gov.justice.digital.test.ZoneFixtures.*;

@ExtendWith(MockitoExtension.class)
class RawZoneTest extends BaseSparkTest {

    private static final JobArguments jobArguments =
            new JobArguments(Collections.singletonMap(JobArguments.RAW_S3_PATH, RAW_PATH));

    @Mock
    private SourceReference mockSourceReference;

    @Mock
    private DataStorageService mockDataStorageService;

    @Mock
    private SourceReferenceService mockSourceReferenceService;

    Dataset<Row> testRecords = createTestDataset(spark);

    @Test
    public void processShouldCreateAndAppendDistinctRawRecords() throws DataStorageException {
        val expectedRecords = createExpectedRawDataset(spark);

        val rawPath = createValidatedPath(RAW_PATH, TABLE_SOURCE, TABLE_NAME);

        when(mockSourceReferenceService.getSourceReference(TABLE_SOURCE, TABLE_NAME))
                .thenReturn(Optional.of(mockSourceReference));

        doNothing()
                .when(mockDataStorageService)
                .appendDistinct(eq(rawPath), refEq(expectedRecords), any());

        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);

        val underTest = new RawZone(
                jobArguments,
                mockDataStorageService,
                mockSourceReferenceService
        );

        assertIterableEquals(
                expectedRecords.collectAsList(),
                underTest.process(spark, testRecords, dataMigrationEventRow).collectAsList()
        );
    }

}
