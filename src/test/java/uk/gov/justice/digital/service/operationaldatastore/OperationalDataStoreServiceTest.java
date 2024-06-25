package uk.gov.justice.digital.service.operationaldatastore;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.datahub.model.SourceReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OperationalDataStoreServiceTest {
    private static final String destinationTableName = "somesource.sometable";
    @Mock
    private OperationalDataStoreDataTransformation mockDataTransformation;
    @Mock
    private OperationalDataStoreDataAccess mockDataAccess;

    @Mock
    private Dataset<Row> inputDataframe;
    @Mock
    private Dataset<Row> transformedDataframe;
    @Mock
    private SourceReference sourceReference;
    @Mock

    private OperationalDataStoreService underTest;

    @BeforeEach
    public void setup() {
        underTest = new OperationalDataStoreService(mockDataTransformation, mockDataAccess);
    }

    @Test
    public void shouldTransformInputDataframe() {
        StructField[] fields = {new StructField("", StringType$.MODULE$, true, null)};
        when(sourceReference.getFullyQualifiedTableName()).thenReturn(destinationTableName);
        when(sourceReference.getSchema()).thenReturn(new StructType(fields));
        when(mockDataTransformation.transform(any(), any())).thenReturn(transformedDataframe);

        underTest.storeBatchData(inputDataframe, sourceReference);

        verify(mockDataTransformation, times(1)).transform(eq(inputDataframe), eq(fields));
    }

    @Test
    public void shouldWriteTransformedDataframeToDestinationTable() {
        StructField[] fields = {new StructField("", StringType$.MODULE$, true, null)};
        when(sourceReference.getFullyQualifiedTableName()).thenReturn(destinationTableName);
        when(sourceReference.getSchema()).thenReturn(new StructType(fields));
        when(mockDataTransformation.transform(any(), any())).thenReturn(transformedDataframe);

        underTest.storeBatchData(inputDataframe, sourceReference);

        verify(mockDataAccess, times(1)).overwriteTable(eq(transformedDataframe), eq(destinationTableName));
    }
}