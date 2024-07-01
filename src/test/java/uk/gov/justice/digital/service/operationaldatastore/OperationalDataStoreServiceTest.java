package uk.gov.justice.digital.service.operationaldatastore;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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

@ExtendWith(MockitoExtension.class)
class OperationalDataStoreServiceTest {
    private static final String destinationTableName = "somesource.sometable";
    @Mock
    private OperationalDataStoreTransformation mockDataTransformation;
    @Mock
    private OperationalDataStoreDataAccess mockDataAccess;

    @Mock
    private Dataset<Row> inputDataframe;
    @Mock
    private Dataset<Row> transformedDataframe;
    @Mock
    private SourceReference sourceReference;

    private OperationalDataStoreServiceImpl underTest;

    @BeforeEach
    public void setup() {
        underTest = new OperationalDataStoreServiceImpl(mockDataTransformation, mockDataAccess);
    }

    @Test
    void shouldTransformInputDataframe() {
        when(sourceReference.getFullyQualifiedTableName()).thenReturn(destinationTableName);
        when(mockDataTransformation.transform(any())).thenReturn(transformedDataframe);

        underTest.storeBatchData(inputDataframe, sourceReference);

        verify(mockDataTransformation, times(1)).transform(inputDataframe);
    }

    @Test
    void shouldWriteTransformedDataframeToDestinationTable() {
        when(sourceReference.getFullyQualifiedTableName()).thenReturn(destinationTableName);
        when(mockDataTransformation.transform(any())).thenReturn(transformedDataframe);

        underTest.storeBatchData(inputDataframe, sourceReference);

        verify(mockDataAccess, times(1)).overwriteTable(transformedDataframe, destinationTableName);
    }
}