package uk.gov.justice.digital.service.operationaldatastore;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreConnectionDetails;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreCredentials;

import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OperationalDataStoreDataAccessTest {
    @Mock
    private OperationalDataStoreConnectionDetailsService connectionDetailsService;
    @Mock
    private Dataset<Row> dataframe;
    @Mock
    private DataFrameWriter<Row> dataframeWriter;

    private OperationalDataStoreDataAccess underTest;

    @Test
    public void shouldRetrieveConnectionDetailsInConstructor() {
        OperationalDataStoreCredentials credentials = new OperationalDataStoreCredentials();
        credentials.setUsername("username");
        credentials.setPassword("password");
        OperationalDataStoreConnectionDetails connectionDetails = new OperationalDataStoreConnectionDetails(
                "jdbc-url", credentials
        );

        when(connectionDetailsService.getConnectionDetails()).thenReturn(connectionDetails);

        underTest = new OperationalDataStoreDataAccess(connectionDetailsService);

        verify(connectionDetailsService, times(1)).getConnectionDetails();
    }

    @Test
    public void shouldOverwriteExistingTable() {
        OperationalDataStoreCredentials credentials = new OperationalDataStoreCredentials();
        credentials.setUsername("username");
        credentials.setPassword("password");
        String destinationTableName = "some.table";
        OperationalDataStoreConnectionDetails connectionDetails = new OperationalDataStoreConnectionDetails(
                "jdbc-url", credentials
        );

        when(connectionDetailsService.getConnectionDetails()).thenReturn(connectionDetails);
        when(dataframe.write()).thenReturn(dataframeWriter);
        when(dataframeWriter.mode(any(SaveMode.class))).thenReturn(dataframeWriter);

        underTest = new OperationalDataStoreDataAccess(connectionDetailsService);
        underTest.overwriteTable(dataframe, destinationTableName);

        Properties expectedProperties = new Properties();
        expectedProperties.put("user", "username");
        expectedProperties.put("password", "password");

        verify(dataframeWriter, times(1)).mode(eq(SaveMode.Overwrite));
        verify(dataframeWriter, times(1))
                .jdbc(eq("jdbc-url"), eq(destinationTableName), eq(expectedProperties));
    }
}