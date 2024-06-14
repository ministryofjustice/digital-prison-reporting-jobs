package uk.gov.justice.digital.zone.operational;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.glue.GlueClient;
import uk.gov.justice.digital.client.secretsmanager.SecretsManagerClient;
import uk.gov.justice.digital.client.secretsmanager.SecretsManagerClientProvider;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreCredentials;
import uk.gov.justice.digital.datahub.model.SourceReference;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.common.CommonDataFields.withMetadataFields;

@Singleton
public class OperationalZoneCDCRecordByRecord implements OperationalZone, Serializable {

    private static final long serialVersionUID = -7722232195505893260L;

    private static final Logger logger = LoggerFactory.getLogger(OperationalZoneCDCRecordByRecord.class);

    private final String url;
    private final String user;
    private final String password;

    @Inject
    public OperationalZoneCDCRecordByRecord(GlueClient glueClient) {
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

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference) {
        val startTime = System.currentTimeMillis();
        String sourceName = sourceReference.getSource();
        String tableName = sourceReference.getTable();
        logger.info("Processing records for Operational Data Store table {}.{}", sourceName, tableName);

        // Normalise columns to lower case to avoid having to quote every column due to Postgres lower casing everything in incoming queries
        Column[] lowerCaseCols = Arrays.stream(dataFrame.columns()).map(colName -> col(colName).as(colName.toLowerCase())).toArray(Column[]::new);
        Dataset<Row> lowerCaseColsDf = dataFrame.select(lowerCaseCols);

        StructType schema = sourceReference.getSchema();
        Collection<String> pkColumns = sourceReference.getPrimaryKey().getKeyColumnNames().stream().map(String::toLowerCase).collect(Collectors.toSet());
        String[] lowerCaseFieldNames = Arrays.stream(schema.fieldNames()).map(String::toLowerCase).toArray(String[]::new);
        String insertColumnNames = String.join(", ", lowerCaseFieldNames);
        String insertValues = String.join(", ", Arrays.stream(lowerCaseFieldNames).map(c -> "s." + c).toArray(String[]::new));
        String updateAssignments = String.join(", ", Arrays.stream(lowerCaseFieldNames).filter(c -> !pkColumns.contains(c)).map(c -> c + " = s." + c).toArray(String[]::new));

        String joinCondition = sourceReference.getPrimaryKey().getSparkCondition("s", "d").toLowerCase();

        StructField[] fields = withMetadataFields(schema).fields();
        // TODO: Remove _timestamp column but keep op column
        String sourceColumns = String.join(", ", Arrays.stream(fields).map(f -> "? as " + f.name().toLowerCase()).toArray(String[]::new));

        lowerCaseColsDf.foreachPartition(partition -> {
            val partitionStartTime = System.currentTimeMillis();
            logger.debug("Processing partition for Operational Data Store table {}.{}", sourceName, tableName);

            // TODO: manage connections?
            Connection connection = DriverManager.getConnection(url, user, password);


            String targetTableName = format("%s.%s", sourceName, tableName);


            String sql = format("WITH s AS (SELECT %s)\n" +
                            "MERGE INTO %s d\n" +
                            "USING s ON %s\n" +
                            "    WHEN MATCHED AND s.op = 'D' THEN DELETE\n" +
                            "    WHEN MATCHED AND s.op = 'U' THEN UPDATE SET %s\n" +
                            "WHEN NOT MATCHED AND (s.op = 'I' OR s.op = 'U') THEN INSERT (%s) VALUES (%s)",
                    sourceColumns, targetTableName, joinCondition, updateAssignments, insertColumnNames, insertValues);
            logger.debug("SQL is {}", sql);
            PreparedStatement ps = connection.prepareStatement(sql);
            partition.forEachRemaining(row -> {
                try {
                    for (int i = 0; i < fields.length; i++) {
                        StructField field = fields[i];
                        DataType dataType = field.dataType();
                        int jdbcIndex = i + 1;
                        populateFieldInStatement(row, dataType, field, ps, jdbcIndex);
                    }
                    ps.addBatch();
                } catch (SQLException e) {
                    logger.error("Exception during creation of JDBC batch", e);
                    throw new RuntimeException(e);
                }
            });
            ps.executeBatch();
            logger.debug("Processed partition for Operational Data Store table {}.{} in {}ms", sourceName, tableName, System.currentTimeMillis() - partitionStartTime);
        });
        logger.info("Processed batch for Operational Data Store table {}.{} in {}ms", sourceName, tableName, System.currentTimeMillis() - startTime);
        return dataFrame;
    }

    private static void populateFieldInStatement(Row row, DataType dataType, StructField field, PreparedStatement ps, int i) throws SQLException {
        String fieldName = field.name().toLowerCase();
        if (dataType instanceof StringType) {
            String data = row.getAs(fieldName);
            if (data == null) {
                ps.setNull(i, Types.VARCHAR);
            } else {
                ps.setString(i, data);
            }
        } else if (dataType instanceof IntegerType) {
            Integer data = row.getAs(fieldName);
            if (data == null) {
                ps.setNull(i, Types.INTEGER);
            } else {
                ps.setInt(i, data);
            }
        } else if (dataType instanceof LongType) {
            Long data = row.getAs(fieldName);
            if (data == null) {
                ps.setNull(i, Types.BIGINT);
            } else {
                ps.setLong(i, data);
            }
        } else if (dataType instanceof DoubleType) {
            Double data = row.getAs(fieldName);
            if (data == null) {
                ps.setNull(i, Types.DOUBLE);
            } else {
                ps.setDouble(i, data);
            }
        } else if (dataType instanceof FloatType) {
            Float data = row.getAs(fieldName);
            if (data == null) {
                ps.setNull(i, Types.FLOAT);
            } else {
                ps.setFloat(i, data);
            }
        } else if (dataType instanceof ShortType) {
            Short data = row.getAs(fieldName);
            if (data == null) {
                ps.setNull(i, Types.SMALLINT);
            } else {
                ps.setShort(i, data);
            }
        } else if (dataType instanceof ByteType) {
            Byte data = row.getAs(fieldName);
            if (data == null) {
                ps.setNull(i, Types.TINYINT);
            } else {
                ps.setByte(i, data);
            }
        } else if (dataType instanceof BooleanType) {
            Boolean data = row.getAs(fieldName);
            if (data == null) {
                ps.setNull(i, Types.BOOLEAN);
            } else {
                ps.setBoolean(i, data);
            }
        } else if (dataType instanceof BinaryType) {
            byte[] data = row.getAs(fieldName);
            if (data == null) {
                ps.setNull(i, Types.BINARY);
            } else {
                ps.setBytes(i, data);
            }
        } else if (dataType instanceof TimestampType) {
            Timestamp data = row.getAs(fieldName);
            if (data == null) {
                ps.setNull(i, Types.TIMESTAMP);
            } else {
                ps.setTimestamp(i, data);
            }
        } else if (dataType instanceof DateType) {
            Date data = row.getAs(fieldName);
            if (data == null) {
                ps.setNull(i, Types.DATE);
            } else {
                ps.setDate(i, data);
            }
        } else if (dataType instanceof DecimalType) {
            BigDecimal data = row.getAs(fieldName);
            if (data == null) {
                ps.setNull(i, Types.BIGINT);
            } else {
                ps.setBigDecimal(i, data);
            }
        } else {
            throw new IllegalStateException(dataType + " is an unrecognized type");
        }
    }


}
