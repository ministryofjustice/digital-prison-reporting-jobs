package uk.gov.justice.digital.zone;


import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.zone.Fixtures.*;

@ExtendWith(MockitoExtension.class)
class StructuredZoneTest extends BaseSparkTest {

    @Mock
    private Dataset<Row> mockDataSet;

    @Mock
    private JobArguments mockJobArguments;

    @Mock
    private SourceReference mockSourceReference;

    @Mock
    private DataStorageService mockDataStorage;

    private MockedStatic<SourceReferenceService> mockSourceReferenceService;

    private StructuredZone underTest;

    @BeforeEach
    void setUp() {
        mockSourceReferenceService = mockStatic(SourceReferenceService.class);
        mockSourceReferenceService.when(() -> SourceReferenceService.generateKey(TABLE_SOURCE, TABLE_NAME)).thenCallRealMethod();
        mockSourceReferenceService.when(() -> SourceReferenceService.getSourceReference(TABLE_SOURCE, TABLE_NAME)).thenCallRealMethod();
        when(mockJobArguments.getViolationsS3Path()).thenReturn(VIOLATIONS_PATH);
        when(mockJobArguments.getStructuredS3Path()).thenReturn(STRUCTURED_PATH);
        underTest = new StructuredZone(mockJobArguments, mockDataStorage);
    }

    @AfterEach
    void tearDown() {
        mockSourceReferenceService.close();
    }

    @Test
    void shouldHandleValidRecords() throws DataStorageException {
        givenTheDatasetSupportsTheProcessFlow();
        assertNotNull(underTest.process(spark, mockDataSet, dataMigrationEventRow));
    }

    @Test
    void shouldHandleInvalidRecords() throws DataStorageException {
        givenTheDatasetSupportsTheProcessFlow();
        assertNotNull(underTest.process(spark, mockDataSet, dataMigrationEventRow));
    }

    @Test
    void shouldHandleSchemaFound() throws DataStorageException {
        givenTheDatasetSupportsTheProcessFlow();
        givenTheSourceReferenceIsValid();
        assertNotNull(underTest.handleSchemaFound(spark, mockDataSet, mockSourceReference));
    }

    @Test
    void shouldHandleNoSchemaFound() throws DataStorageException {
        givenTheDatasetSupportsTheNoSchemaFoundFlow();
        assertNotNull(underTest.handleNoSchemaFound(spark, mockDataSet, TABLE_SOURCE, TABLE_NAME));
    }

    private void givenTheDatasetSupportsTheProcessFlow() {
        when(mockDataSet.select(Mockito.<Column[]>any())).thenReturn(mockDataSet);
        when(mockDataSet.select(Mockito.<String>any())).thenReturn(mockDataSet);
        when(mockDataSet.filter(Mockito.<Column>any())).thenReturn(mockDataSet);
        when(mockDataSet.drop(Mockito.<Column>any())).thenReturn(mockDataSet);
        when(mockDataSet.withColumn(any(), any())).thenReturn(mockDataSet);
        when(mockDataSet.count()).thenReturn(10L);
    }

    private void givenTheDatasetSupportsTheNoSchemaFoundFlow() {
        when(mockDataSet.sparkSession()).thenReturn(spark);
        when(mockDataSet.schema()).thenReturn(ROW_SCHEMA);
        when(mockDataSet.select(Mockito.<Column[]>any())).thenReturn(mockDataSet);
        when(mockDataSet.withColumn(any(), any())).thenReturn(mockDataSet);
        when(mockDataSet.count()).thenReturn(10L);
    }

    private void givenTheSourceReferenceIsValid() {
        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);
        when(mockSourceReference.getSchema()).thenReturn(ROW_SCHEMA);
    }
}