package uk.gov.justice.digital.zone.operational;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.glue.GlueClient;
import uk.gov.justice.digital.client.secretsmanager.SecretsManagerClient;
import uk.gov.justice.digital.client.secretsmanager.SecretsManagerClientProvider;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreCredentials;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.zone.Zone;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;

@Singleton
public class OperationalZoneLoad implements Zone  {

    private static final Logger logger = LoggerFactory.getLogger(OperationalZoneLoad.class);

    private final String url;
    private final String user;
    private final String password;

    @Inject
    public OperationalZoneLoad(GlueClient glueClient) {
        com.amazonaws.services.glue.model.Connection connection = glueClient.getConnection("Postgresql connection");
        Map<String, String> connectionProperties = connection.getConnectionProperties();
        url = connectionProperties.get("JDBC_CONNECTION_URL");
        String secretId = connectionProperties.get("SECRET_ID");
        // TODO Dependency injection, etc.
        SecretsManagerClient secretsManagerClient = new SecretsManagerClient(new SecretsManagerClientProvider());
        OperationalDataStoreCredentials creds = secretsManagerClient.getSecret(secretId, OperationalDataStoreCredentials.class);
        user = creds.getUsername();
        password = creds.getPassword();
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference) {

        String sourceName = sourceReference.getSource();
        String tableName = sourceReference.getTable();
        String destinationTableName = sourceName + "." + tableName;

        logger.info("Processing records for Operational Data Store table {}", destinationTableName);

        // Normalise columns to lower case to avoid having to quote every column due to Postgres lower casing everything in incoming queries
        Column[] lowerCaseCols = Arrays.stream(dataFrame.columns()).map(colName -> col(colName).as(colName.toLowerCase())).toArray(Column[]::new);
        Dataset<Row> lowerCaseColsDf = dataFrame.select(lowerCaseCols);

        // Handle 0x00 null String character which cannot be inserted in to a Postgres text column
        for (StructField field : sourceReference.getSchema().fields()) {
            if (field.dataType() instanceof StringType) {
                String columnname = field.name().toLowerCase();
                lowerCaseColsDf = lowerCaseColsDf.withColumn(columnname, regexp_replace(lowerCaseColsDf.col(columnname), null, ""));
            }
        }

        Properties props = new Properties();
        props.put("user", user);
        props.put("password", password);


        lowerCaseColsDf.write().mode(SaveMode.Overwrite).jdbc(url, destinationTableName, props);

        return dataFrame;
    }
}
