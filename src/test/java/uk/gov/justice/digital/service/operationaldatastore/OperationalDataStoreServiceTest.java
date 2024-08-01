package uk.gov.justice.digital.service.operationaldatastore;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.OperationalDataStoreException;
import uk.gov.justice.digital.service.operationaldatastore.dataaccess.OperationalDataStoreDataAccess;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.CHECKPOINT_COL;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.TIMESTAMP;

@ExtendWith(MockitoExtension.class)
class OperationalDataStoreServiceTest {
    private static final String NAMESPACE = "prisons";
    private static final String SOURCE_NAME = "somesource";
    private static final String TABLE_NAME = "sometable";
    private static final String EXPECTED_FULL_TABLE_NAME = "prisons.somesource_sometable";
    private static final String EXPECTED_LOADING_FULL_TABLE_NAME = "loading.somesource_sometable";

    private static final StructType schema = new StructType(new StructField[]{
            new StructField("PK", DataTypes.StringType, true, Metadata.empty()),
            new StructField(TIMESTAMP, DataTypes.StringType, true, Metadata.empty()),
            new StructField(OPERATION, DataTypes.StringType, true, Metadata.empty()),
            new StructField(CHECKPOINT_COL, DataTypes.StringType, true, Metadata.empty()),
            new StructField("DATA", DataTypes.StringType, true, Metadata.empty())
    });

    @Mock
    private OperationalDataStoreTransformation mockDataTransformation;
    @Mock
    private OperationalDataStoreDataAccess mockDataAccess;
    @Mock
    private JobArguments jobArguments;

    @Mock
    private Dataset<Row> inputDataframe;
    @Mock
    private Dataset<Row> transformedDataframe;
    @Mock
    private Dataset<Row> colsDroppedDataframe;
    @Mock
    private SourceReference sourceReference;

    private OperationalDataStoreService underTest;

    @BeforeEach
    public void setup() {
        when(jobArguments.getOperationalDataStoreLoadingSchemaName()).thenReturn("loading");
        underTest = new OperationalDataStoreServiceImpl(jobArguments, mockDataTransformation, mockDataAccess);
        when(sourceReference.getSource()).thenReturn(SOURCE_NAME);
        when(sourceReference.getTable()).thenReturn(TABLE_NAME);
        when(sourceReference.getNamespace()).thenReturn(NAMESPACE);
    }

    @Test
    void overwriteDataShouldTransformInputDataframe() {
        when(mockDataTransformation.transform(any())).thenReturn(transformedDataframe);
        when(mockDataAccess.isOperationalDataStoreManagedTable(any())).thenReturn(true);
        when(mockDataAccess.tableExists(any())).thenReturn(true);

        underTest.overwriteData(inputDataframe, sourceReference);

        verify(mockDataTransformation, times(1)).transform(inputDataframe);
    }

    @Test
    void overwriteDataShouldWriteTransformedDataframeToDestinationTableAfterDroppingMetadataCols() {
        when(mockDataTransformation.transform(any())).thenReturn(transformedDataframe);
        when(transformedDataframe.drop((String[]) any())).thenReturn(colsDroppedDataframe);
        when(mockDataAccess.isOperationalDataStoreManagedTable(any())).thenReturn(true);
        when(mockDataAccess.tableExists(any())).thenReturn(true);

        underTest.overwriteData(inputDataframe, sourceReference);

        verify(transformedDataframe, times(1)).drop("op", "_timestamp", "checkpoint_col");
        verify(mockDataAccess, times(1)).overwriteTable(colsDroppedDataframe, EXPECTED_FULL_TABLE_NAME);
    }

    @Test
    void overwriteDataShouldSkipOverwriteForTablesUnmanagedByOperationalDataStore() {
        when(mockDataAccess.isOperationalDataStoreManagedTable(any())).thenReturn(false);

        underTest.overwriteData(inputDataframe, sourceReference);

        verify(mockDataAccess, times(0)).overwriteTable(colsDroppedDataframe, EXPECTED_FULL_TABLE_NAME);
    }

    @Test
    void overwriteDataShouldThrowIfTheTableDoesNotExist() {
        when(mockDataAccess.isOperationalDataStoreManagedTable(any())).thenReturn(true);
        when(mockDataAccess.tableExists(any())).thenReturn(false);

        assertThrows(OperationalDataStoreException.class, () -> {
            underTest.overwriteData(inputDataframe, sourceReference);
        });

        verify(mockDataAccess, times(0)).overwriteTable(colsDroppedDataframe, EXPECTED_FULL_TABLE_NAME);
    }

    @Test
    void mergeDataShouldTransformInputDataframe() {
        when(mockDataTransformation.transform(any())).thenReturn(transformedDataframe);
        when(mockDataAccess.isOperationalDataStoreManagedTable(any())).thenReturn(true);

        underTest.mergeData(inputDataframe, sourceReference);

        verify(mockDataTransformation, times(1)).transform(inputDataframe);
    }

    @Test
    void mergeDataShouldWriteTransformedDataframeToLoadingTableAfterDroppingMetadataCols() {
        when(mockDataTransformation.transform(any())).thenReturn(transformedDataframe);
        when(transformedDataframe.drop((String[]) any())).thenReturn(colsDroppedDataframe);
        when(mockDataAccess.isOperationalDataStoreManagedTable(any())).thenReturn(true);

        underTest.mergeData(inputDataframe, sourceReference);

        verify(transformedDataframe, times(1)).drop("_timestamp", "checkpoint_col");
        verify(mockDataAccess, times(1)).overwriteTable(colsDroppedDataframe, EXPECTED_LOADING_FULL_TABLE_NAME);
    }

    @Test
    void mergeDataShouldRunMerge() {
        when(mockDataTransformation.transform(any())).thenReturn(transformedDataframe);
        when(transformedDataframe.drop((String[]) any())).thenReturn(colsDroppedDataframe);
        when(mockDataAccess.isOperationalDataStoreManagedTable(any())).thenReturn(true);

        underTest.mergeData(inputDataframe, sourceReference);

        verify(mockDataAccess, times(1)).merge(EXPECTED_LOADING_FULL_TABLE_NAME, EXPECTED_FULL_TABLE_NAME, sourceReference);
    }

    @Test
    void mergeDataShouldSkipOverwriteForTablesUnmanagedByOperationalDataStore() {
        when(mockDataAccess.isOperationalDataStoreManagedTable(any())).thenReturn(false);

        underTest.mergeData(inputDataframe, sourceReference);

        verify(mockDataAccess, times(0)).overwriteTable(colsDroppedDataframe, EXPECTED_FULL_TABLE_NAME);
    }
}