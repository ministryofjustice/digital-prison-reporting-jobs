package uk.gov.justice.digital.service.operationaldatastore;

import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreConnectionDetails;

import java.util.Properties;

/**
 * Responsible for accessing the Operational DataStore.
 */
@Singleton
public class OperationalDataStoreDataAccess {

    private static final Logger logger = LoggerFactory.getLogger(OperationalDataStoreDataAccess.class);

    private final String jdbcUrl;
    private final Properties jdbcProps;

    public OperationalDataStoreDataAccess(OperationalDataStoreConnectionDetailsService connectionDetailsService) {
        logger.debug("Retrieving connection details for Operational DataStore");
        OperationalDataStoreConnectionDetails connectionDetails = connectionDetailsService.getConnectionDetails();
        jdbcUrl = connectionDetails.getUrl();
        jdbcProps = new Properties();
        jdbcProps.put("driver", connectionDetails.getJdbcDriverClassName());
        jdbcProps.put("user", connectionDetails.getCredentials().getUsername());
        jdbcProps.put("password", connectionDetails.getCredentials().getPassword());
        logger.debug("Finished retrieving connection details for Operational DataStore");
    }

    void overwriteTable(Dataset<Row> dataframe, String destinationTableName) {
        val startTime = System.currentTimeMillis();
        logger.debug("Writing data to Operational DataStore");
        dataframe.write()
                .mode(SaveMode.Overwrite)
                .jdbc(jdbcUrl, destinationTableName, jdbcProps);
        logger.debug("Finished writing data to Operational DataStore in {}ms", System.currentTimeMillis() - startTime);
    }
}
