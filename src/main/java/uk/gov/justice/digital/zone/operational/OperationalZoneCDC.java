package uk.gov.justice.digital.zone.operational;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.glue.GlueClient;
import uk.gov.justice.digital.client.secretsmanager.SecretsManagerClient;
import uk.gov.justice.digital.client.secretsmanager.SecretsManagerClientProvider;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreCredentials;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.zone.Zone;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;

@Singleton
public class OperationalZoneCDC implements Zone {

    private static final Logger logger = LoggerFactory.getLogger(OperationalZoneCDC.class);
    private static final String LOADING_SCHEMA = "loading";

    private final String url;
    private final String user;
    private final String password;

    @Inject
    public OperationalZoneCDC(GlueClient glueClient) {
        // TODO: Meaningful connection name and grab it from configuration
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

    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference) {
        val startTime = System.currentTimeMillis();
        String sourceName = sourceReference.getSource();
        String tableName = sourceReference.getTable();
        logger.info("Processing records for Operational Data Store table {}.{}", sourceName, tableName);

        String temporaryTableName = LOADING_SCHEMA + "." + tableName;
        logger.debug("Loading to temporary table {}", temporaryTableName);
        Properties props = new Properties();
        props.put("user", user);
        props.put("password", password);

        // Normalise columns to lower case to avoid having to quote every column due to Postgres lower casing everything in incoming queries
        Column[] lowerCaseCols = Arrays.stream(dataFrame.columns()).map(colName -> col(colName).as(colName.toLowerCase())).toArray(Column[]::new);
        Dataset<Row> lowerCaseColsDf = dataFrame.select(lowerCaseCols);

        // Handle 0x00 null String character which cannot be inserted in to a Postgres text column
        for (StructField field : sourceReference.getSchema().fields()) {
            if (field.dataType() instanceof StringType) {
                String columnname = field.name().toLowerCase();
                lowerCaseColsDf = lowerCaseColsDf.withColumn(columnname, regexp_replace(lowerCaseColsDf.col(columnname), "\u0000", ""));
            }
        }

        lowerCaseColsDf.write().mode(SaveMode.Overwrite).jdbc(url, temporaryTableName, props);
        logger.debug("Finished loading to temporary table {}", temporaryTableName);

        StructType schema = sourceReference.getSchema();
        Collection<String> pkColumns = sourceReference.getPrimaryKey().getKeyColumnNames().stream().map(String::toLowerCase).collect(Collectors.toSet());
        String[] lowerCaseFieldNames = Arrays.stream(schema.fieldNames()).map(String::toLowerCase).toArray(String[]::new);
        String insertColumnNames = String.join(", ", lowerCaseFieldNames);
        String insertValues = String.join(", ", Arrays.stream(lowerCaseFieldNames).map(c -> "s." + c).toArray(String[]::new));
        String updateAssignments = String.join(", ", Arrays.stream(lowerCaseFieldNames).filter(c -> !pkColumns.contains(c)).map(c -> c + " = s." + c).toArray(String[]::new));

        String joinCondition = sourceReference.getPrimaryKey().getSparkCondition("s", "d").toLowerCase();

        String destinationTableName = sourceName + "." + tableName;
        logger.debug("Merging into destination table {}", destinationTableName);
        String mergeSql = format("MERGE INTO %s d\n" +
                        "USING %s s ON %s\n" +
                        "    WHEN MATCHED AND s.op = 'D' THEN DELETE\n" +
                        "    WHEN MATCHED AND s.op = 'U' THEN UPDATE SET %s\n" +
                        "    WHEN NOT MATCHED AND (s.op = 'I' OR s.op = 'U') THEN INSERT (%s) VALUES (%s)",
                destinationTableName, temporaryTableName, joinCondition, updateAssignments, insertColumnNames, insertValues);
        logger.debug("Merge SQL is {}", mergeSql);
        String truncateSql = format("TRUNCATE TABLE %s", temporaryTableName);
        logger.debug("Truncate SQL is {}", truncateSql);
        // TODO: manage connections?
        try {
            Connection connection = DriverManager.getConnection(url, user, password);
            connection.createStatement().execute(mergeSql);
            logger.debug("Finished merging into destination table {}", destinationTableName);
            // Truncation of the temporary loading is not really required since spark will do it on next load.
            // We do it just to keep space free.
            connection.createStatement().execute(truncateSql);
            logger.debug("Finished truncating temporary table {}", temporaryTableName);
        } catch (SQLException e) {
            logger.error("Exception during merge from temporary table to destination", e);
            throw new RuntimeException(e);
        }

        logger.info("Processed batch for Operational Data Store table {}.{} in {}ms", sourceName, tableName, System.currentTimeMillis() - startTime);
        return dataFrame;
    }
}
