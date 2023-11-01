package uk.gov.justice.digital.zone.curated;


import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ViolationService;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.TIMESTAMP;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.CURATED_LOAD;
import static uk.gov.justice.digital.test.Fixtures.CURATED_PATH;
import static uk.gov.justice.digital.test.Fixtures.PRIMARY_KEY_FIELD;
import static uk.gov.justice.digital.test.Fixtures.TABLE_NAME;
import static uk.gov.justice.digital.test.Fixtures.TABLE_SOURCE;
import static uk.gov.justice.digital.test.ZoneFixtures.createStructuredLoadDataset;

@ExtendWith(MockitoExtension.class)
class CuratedZoneLoadTest extends BaseSparkTest {

    @Mock
    private JobArguments mockJobArguments;

    @Mock
    private SourceReference mockSourceReference;

    @Mock
    private DataStorageService mockDataStorage;
    @Mock
    private ViolationService mockViolationService;

    @Captor
    ArgumentCaptor<Dataset<Row>> dataframeCaptor;

    private final Dataset<Row> testDataSet = createStructuredLoadDataset(spark);

    private final String curatedPath = createValidatedPath(CURATED_PATH, TABLE_SOURCE, TABLE_NAME);

    private final SourceReference.PrimaryKey primaryKey = new SourceReference.PrimaryKey(PRIMARY_KEY_FIELD);

    private CuratedZone underTest;


    @BeforeEach
    public void setUp() {
        reset(mockDataStorage);
        when(mockJobArguments.getCuratedS3Path()).thenReturn(CURATED_PATH);

        underTest = new CuratedZoneLoad(
                mockJobArguments,
                mockDataStorage,
                mockViolationService
        );
    }

    @Test
    public void shouldWriteStructuredLoadRecordsToDeltaTable() throws DataStorageException {
        givenTheSourceReferenceIsValid();
        doNothing()
                .when(mockDataStorage)
                .appendDistinct(eq(curatedPath), dataframeCaptor.capture(), eq(primaryKey));

        assertIterableEquals(
                testDataSet.collectAsList(),
                underTest.process(spark, testDataSet, mockSourceReference).collectAsList()
        );

        assertIterableEquals(
                testDataSet.drop(OPERATION, TIMESTAMP).collectAsList(),
                dataframeCaptor.getValue().collectAsList()
        );
    }

    @Test
    public void shouldWriteViolationsWhenDataStorageRetriesExhausted() throws DataStorageException {
        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);

        DataStorageRetriesExhaustedException thrown = new DataStorageRetriesExhaustedException(new Exception("Some problem"));
        doThrow(thrown).when(mockDataStorage).appendDistinct(any(), any(), any());

        underTest.process(spark, testDataSet, mockSourceReference).collect();
        verify(mockViolationService).handleRetriesExhausted(any(), eq(testDataSet), eq(TABLE_SOURCE), eq(TABLE_NAME), eq(thrown), eq(CURATED_LOAD));
    }

    @Test
    public void shouldReturnEmptyDataFrameWhenDataStorageRetriesExhausted() throws DataStorageException {
        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);

        DataStorageRetriesExhaustedException thrown = new DataStorageRetriesExhaustedException(new Exception("Some problem"));
        doThrow(thrown).when(mockDataStorage).appendDistinct(any(), any(), any());

        val resultDf = underTest.process(spark, testDataSet, mockSourceReference);
        assertTrue(resultDf.isEmpty());
    }

    private void givenTheSourceReferenceIsValid() {
        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);
        when(mockSourceReference.getPrimaryKey()).thenReturn(primaryKey);
    }

}