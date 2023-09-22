package uk.gov.justice.digital.zone.structured;


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
import uk.gov.justice.digital.service.DataStorageService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.test.Fixtures.*;
import static uk.gov.justice.digital.test.ZoneFixtures.createStructuredLoadDataset;
import static uk.gov.justice.digital.test.ZoneFixtures.createTestDataset;

@ExtendWith(MockitoExtension.class)
class StructuredZoneLoadTest extends BaseSparkTest {

    @Mock
    private JobArguments mockJobArguments;

    @Mock
    private SourceReference mockSourceReference;

    @Mock
    private DataStorageService mockDataStorage;

    @Captor
    ArgumentCaptor<Dataset<Row>> dataframeCaptor;

    private StructuredZone underTest;

    private final Dataset<Row> testDataSet = createTestDataset(spark);

    private final SourceReference.PrimaryKey primaryKey = new SourceReference.PrimaryKey(PRIMARY_KEY_FIELD);


    @BeforeEach
    public void setUp() {
        reset(mockDataStorage);
        when(mockJobArguments.getViolationsS3Path()).thenReturn(VIOLATIONS_PATH);
        when(mockJobArguments.getStructuredS3Path()).thenReturn(STRUCTURED_PATH);

        underTest = new StructuredZoneLoad(
                mockJobArguments,
                mockDataStorage
        );
    }

    @Test
    public void shouldHandleValidRecords() throws DataStorageException {
        val expectedRecords = createStructuredLoadDataset(spark);
        val structuredPath = createValidatedPath(STRUCTURED_PATH, TABLE_SOURCE, TABLE_NAME);

        givenTheSourceReferenceIsValid();
        doNothing()
                .when(mockDataStorage)
                .appendDistinct(eq(structuredPath), dataframeCaptor.capture(), any());

        assertIterableEquals(
                expectedRecords.collectAsList(),
                underTest.process(spark, testDataSet, mockSourceReference).collectAsList()
        );

        assertIterableEquals(
                expectedRecords.drop(OPERATION).collectAsList(),
                dataframeCaptor.getValue().collectAsList()
        );
    }

    @Test
    public void shouldHandleInvalidRecords() throws DataStorageException {
        givenTheSourceReferenceIsValid();

        assertNotNull(underTest.process(spark, testDataSet, mockSourceReference));
    }

    @Test
    public void shouldKeepNullColumnsInData() throws DataStorageException {
        givenTheSourceReferenceIsValid();
        doNothing().when(mockDataStorage).appendDistinct(any(), any(), any());

        val structuredLoadRecords = underTest.process(spark, testDataSet, mockSourceReference);

        assertTrue(hasNullColumns(structuredLoadRecords));
    }

    private void givenTheSourceReferenceIsValid() {
        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);
        when(mockSourceReference.getSchema()).thenReturn(JSON_DATA_SCHEMA);
        when(mockSourceReference.getPrimaryKey()).thenReturn(primaryKey);
    }

}