package uk.gov.justice.digital.datamart;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.secretsmanager.SecretsManagerClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.converter.dms.DMS_3_4_6;
import uk.gov.justice.digital.domain.model.RedshiftConfig;
import uk.gov.justice.digital.domain.model.TableDefinition;
import uk.gov.justice.digital.exception.SecretsManagerClientException;
import uk.gov.justice.digital.service.DataStorageService;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;

@Singleton
public class DataMartMapper {

    private static final Logger logger = LoggerFactory.getLogger(DataMartMapper.class);

    private static final String DATABASE_NAME = "datamart";
    private final HashMap<String, String> redshiftOptions = new HashMap<>();
    private final DataStorageService storage;

    @Inject
    public DataMartMapper(
            JobArguments jobArguments,
//            SecretsManagerClient secretsManagerClient,
            DataStorageService storage
    ) throws SecretsManagerClientException {
        this.storage = storage;

//        try {
            val secretsName = jobArguments.getRedshiftSecretsName();
//            val redshiftConfig = secretsManagerClient.getSecret(secretsName, RedshiftConfig.class);
//            val jdbcURL = String.format(
//                    "jdbc:redshift://%s/%s?user=%s&password=%s",
//                    redshiftConfig.getHost(),
//                    DATABASE_NAME,
//                    redshiftConfig.getUsername(),
//                    redshiftConfig.getPassword()
//            );
//
//            redshiftOptions.put("url", jdbcURL);
//        } catch (SecretsManagerClientException ex) {
//            logger.error("Failed to initialize redshift connection", ex);
//            throw ex;
//        }
    }

    public void mapToRedshift(
            Dataset<Row> dataFrame,
            String domainName,
            TableDefinition tableDefinition,
            String primaryKeyValue,
            DMS_3_4_6.Operation operation
    ) {
        val tableName = tableDefinition.getName();
        val dataMartTableName = domainName + "_" + tableName;
        val primaryKey = tableDefinition.getPrimaryKey();

        logger.info("Writing domain to {} {}", DATABASE_NAME, dataMartTableName);
        storage.writeToRedshift(dataFrame, redshiftOptions, dataMartTableName, primaryKey, primaryKeyValue, operation);
        logger.info("Domain successfully written to {} {}", DATABASE_NAME, dataMartTableName);
    }
}