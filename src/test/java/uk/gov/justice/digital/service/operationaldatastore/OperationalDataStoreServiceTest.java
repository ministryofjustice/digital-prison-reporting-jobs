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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.CHECKPOINT_COL;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.TIMESTAMP;

@ExtendWith(MockitoExtension.class)
class OperationalDataStoreServiceTest {
    private static final String destinationTableName = "somesource.sometable";

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
    }

    @Test
    void overwriteDataShouldTransformInputDataframe() {
        when(sourceReference.getFullyQualifiedTableName()).thenReturn(destinationTableName);
        when(mockDataTransformation.transform(any())).thenReturn(transformedDataframe);

        underTest.overwriteData(inputDataframe, sourceReference);

        verify(mockDataTransformation, times(1)).transform(inputDataframe);
    }

    @Test
    void overwriteDataShouldWriteTransformedDataframeToDestinationTableAfterDroppingMetadataCols() {
        when(sourceReference.getFullyQualifiedTableName()).thenReturn(destinationTableName);
        when(mockDataTransformation.transform(any())).thenReturn(transformedDataframe);
        when(transformedDataframe.drop((String[]) any())).thenReturn(colsDroppedDataframe);

        underTest.overwriteData(inputDataframe, sourceReference);

        verify(transformedDataframe, times(1)).drop("op", "_timestamp", "checkpoint_col");
        verify(mockDataAccess, times(1)).overwriteTable(colsDroppedDataframe, destinationTableName);
    }

    @Test
    void mergeDataShouldTransformInputDataframe() {
        when(sourceReference.getFullyQualifiedTableName()).thenReturn(destinationTableName);
        when(mockDataTransformation.transform(any())).thenReturn(transformedDataframe);

        underTest.mergeData(inputDataframe, sourceReference);

        verify(mockDataTransformation, times(1)).transform(inputDataframe);
    }

    @Test
    void mergeDataShouldWriteTransformedDataframeToLoadingTableAfterDroppingMetadataCols() {
        when(sourceReference.getFullyQualifiedTableName()).thenReturn(destinationTableName);
        when(sourceReference.getTable()).thenReturn("some_table");
        when(mockDataTransformation.transform(any())).thenReturn(transformedDataframe);
        when(transformedDataframe.drop((String[]) any())).thenReturn(colsDroppedDataframe);

        underTest.mergeData(inputDataframe, sourceReference);

        verify(transformedDataframe, times(1)).drop("_timestamp", "checkpoint_col");
        verify(mockDataAccess, times(1)).overwriteTable(colsDroppedDataframe, "loading.some_table");
    }

    @Test
    void mergeDataShouldRunMerge() {
        when(sourceReference.getFullyQualifiedTableName()).thenReturn(destinationTableName);
        when(sourceReference.getTable()).thenReturn("some_table");
        when(mockDataTransformation.transform(any())).thenReturn(transformedDataframe);
        when(transformedDataframe.drop((String[]) any())).thenReturn(colsDroppedDataframe);

        underTest.mergeData(inputDataframe, sourceReference);

        verify(mockDataAccess, times(1)).merge("loading.some_table", destinationTableName, sourceReference);
    }
}