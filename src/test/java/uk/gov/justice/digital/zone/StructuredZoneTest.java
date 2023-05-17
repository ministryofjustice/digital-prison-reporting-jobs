package uk.gov.justice.digital.zone;


import lombok.val;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
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

import java.util.ArrayList;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.common.ColumnNames.*;
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
    private DataStorageService mockDataStorageService;

    private MockedStatic<SourceReferenceService> mockSourceReferenceService;

    @BeforeEach
    void setUp() {
        mockSourceReferenceService = mockStatic(SourceReferenceService.class);
        // Configure mocks common to most test cases
        when(mockJobArguments.getViolationsS3Path()).thenReturn(VIOLATIONS_PATH);
        when(mockJobArguments.getStructuredS3Path()).thenReturn(STRUCTURED_PATH);
    }

    @AfterEach
    void tearDown() {
        mockSourceReferenceService.close();
    }

    @Test
    void shouldHandleValidRecords() throws DataStorageException {
        val underTest = spy(new StructuredZone(mockJobArguments, mockDataStorageService));

        mockSourceReferenceService.when(() -> SourceReferenceService.generateKey(TABLE_SOURCE, TABLE_NAME)).thenCallRealMethod();
        mockSourceReferenceService.when(() -> SourceReferenceService.getSourceReference(TABLE_SOURCE, TABLE_NAME)).thenCallRealMethod();

        doReturn(mockDataSet).when(underTest).validateJsonData(any(), any(), any(), any(), any());
        doNothing().when(underTest).handleInValidRecords(any(), any(), any(), any(), any());
        doReturn(mockDataSet).when(underTest)
                .handleValidRecords(spark, mockDataSet, STRUCTURED_PATH + "/nomis/agency_internal_locations");
        when(mockDataSet.count()).thenReturn(10L);


        assertNotNull(underTest.process(spark, mockDataSet, dataMigrationEventRow));
    }

    @Test
    void shouldHandleInvalidRecords() throws DataStorageException {
        val underTest = spy(new StructuredZone(mockJobArguments, mockDataStorageService));

        mockSourceReferenceService.when(() -> SourceReferenceService.generateKey(TABLE_SOURCE, TABLE_NAME)).thenCallRealMethod();
        mockSourceReferenceService.when(() -> SourceReferenceService.getSourceReference(TABLE_SOURCE, TABLE_NAME)).thenCallRealMethod();

        when(mockDataSet.select(Mockito.<Column[]>any())).thenReturn(mockDataSet);
        when(mockDataSet.select(Mockito.<String>any())).thenReturn(mockDataSet);
        when(mockDataSet.filter(Mockito.<Column>any())).thenReturn(mockDataSet);
        when(mockDataSet.withColumn(any(), any())).thenReturn(mockDataSet);
        when(mockDataSet.drop(Mockito.<Column>any())).thenReturn(mockDataSet);
        when(mockDataSet.count()).thenReturn(10L);

        assertNotNull(underTest.process(spark, mockDataSet, dataMigrationEventRow));
    }

    @Test
    void shouldHandleSchemaFound() throws DataStorageException {
        val structuredZoneTest = new StructuredZone(mockJobArguments, mockDataStorageService);

        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);
        when(mockSourceReference.getSchema()).thenReturn(ROW_SCHEMA);

        when(mockDataSet.select(Mockito.<Column[]>any())).thenReturn(mockDataSet);
        when(mockDataSet.select(Mockito.<String>any())).thenReturn(mockDataSet);
        when(mockDataSet.filter(Mockito.<Column>any())).thenReturn(mockDataSet);
        when(mockDataSet.withColumn(any(), any())).thenReturn(mockDataSet);
        when(mockDataSet.drop(Mockito.<Column>any())).thenReturn(mockDataSet);
        when(mockDataSet.count()).thenReturn(10L);

        assertNotNull(structuredZoneTest.handleSchemaFound(spark, mockDataSet, mockSourceReference));
    }

    @Test
    void shouldHandleNoSchemaFound() throws DataStorageException {
        val schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField(DATA, StringType, true),
                DataTypes.createStructField(METADATA, StringType, true),
                DataTypes.createStructField(PARSED_DATA, StringType, true),
                DataTypes.createStructField(VALID, StringType, true)
        });
        val df = spark.createDataFrame(new ArrayList<>(), schema);

        mockSourceReferenceService.when(() -> SourceReferenceService.generateKey(TABLE_SOURCE, TABLE_NAME)).thenCallRealMethod();
        mockSourceReferenceService.when(() -> SourceReferenceService.getSourceReference(TABLE_SOURCE, TABLE_NAME)).thenCallRealMethod();

        val sourceRef = SourceReferenceService.getSourceReference(TABLE_SOURCE, TABLE_NAME).get();

        val underTest = spy(new StructuredZone(mockJobArguments, mockDataStorageService));

        assertNotNull(underTest.handleNoSchemaFound(spark, df, sourceRef.getSource(), sourceRef.getTable()));

    }
}