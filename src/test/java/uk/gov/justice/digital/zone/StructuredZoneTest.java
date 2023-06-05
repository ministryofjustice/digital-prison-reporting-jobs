package uk.gov.justice.digital.zone;


import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;
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

    @Mock
    private SourceReferenceService mockSourceReferenceService;

    private StructuredZone underTest;

    @BeforeEach
    public void setUp() {
        when(mockJobArguments.getViolationsS3Path()).thenReturn(VIOLATIONS_PATH);
        when(mockJobArguments.getStructuredS3Path()).thenReturn(STRUCTURED_PATH);

        underTest = new StructuredZone(
                mockJobArguments,
                mockDataStorage,
                mockSourceReferenceService
        );
    }

    @Test
    public void shouldHandleValidRecords() throws DataStorageException {
        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        givenTheDatasetSupportsTheProcessFlow();
        assertNotNull(underTest.process(spark, mockDataSet, dataMigrationEventRow));
    }

    @Test
    public void shouldHandleInvalidRecords() throws DataStorageException {
        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        givenTheDatasetSupportsTheProcessFlow();
        assertNotNull(underTest.process(spark, mockDataSet, dataMigrationEventRow));
    }

    @Test
    public void shouldHandleNoSchemaFound() throws DataStorageException {
        givenTheSchemaDoesNotExist();
        givenTheDatasetSupportsTheNoSchemaFoundFlow();
        assertNotNull(underTest.process(spark, mockDataSet, dataMigrationEventRow));
    }

    private void givenTheSchemaExists() {
        when(mockSourceReferenceService.getSourceReference(TABLE_SOURCE, TABLE_NAME))
                .thenReturn(Optional.of(mockSourceReference));
    }

    private void givenTheSchemaDoesNotExist() {
        when(mockSourceReferenceService.getSourceReference(TABLE_SOURCE, TABLE_NAME)).thenReturn(Optional.empty());
    }

    private void givenTheSourceReferenceIsValid() {
        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);
        when(mockSourceReference.getSchema()).thenReturn(ROW_SCHEMA);
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

}