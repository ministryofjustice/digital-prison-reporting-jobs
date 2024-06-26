package uk.gov.justice.digital.service.operationaldatastore;

import jakarta.inject.Singleton;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreConnectionDetails;

import java.util.Properties;

/**
 * Responsible for accessing the Operational DataStore.
 */
@Singleton
public class OperationalDataStoreDataAccess {

    private final String jdbcUrl;
    private final Properties jdbcProps;

    public OperationalDataStoreDataAccess(OperationalDataStoreConnectionDetailsService connectionDetailsService) {
        OperationalDataStoreConnectionDetails connectionDetails = connectionDetailsService.getConnectionDetails();
        jdbcUrl = connectionDetails.getUrl();
        jdbcProps = new Properties();
        jdbcProps.put("user", connectionDetails.getCredentials().getUsername());
        jdbcProps.put("password", connectionDetails.getCredentials().getPassword());
    }

    void overwriteTable(Dataset<Row> dataframe, String destinationTableName) {
        dataframe.write()
                .mode(SaveMode.Overwrite)
                .jdbc(jdbcUrl, destinationTableName, jdbcProps);
    }
}
