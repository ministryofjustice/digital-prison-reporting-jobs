package uk.gov.justice.digital.zone.operational;

import lombok.val;
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
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.zone.Zone;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import static java.lang.String.format;
import static uk.gov.justice.digital.common.CommonDataFields.withMetadataFields;

public class OperationalZoneCDCRecordByRecord implements Zone {

    private static final Logger logger = LoggerFactory.getLogger(OperationalZoneCDCRecordByRecord.class);

    // TODO: From secrets manager?
    private static final String url = "jdbc:postgresql://localhost:5432/postgres";
    private static final String user = "postgres";
    private static final String password = "password";

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference) {
        val startTime = System.currentTimeMillis();
        String sourceName = sourceReference.getSource();
        String tableName = sourceReference.getTable();
        logger.info("Processing records for Operational Data Store table {}.{}", sourceName, tableName);

        StructType schema = sourceReference.getSchema();
        Collection<String> pkColumns = sourceReference.getPrimaryKey().getKeyColumnNames();
        String insertColumnNames = String.join(", ", schema.fieldNames());
        String insertValues = String.join(", ", Arrays.stream(schema.fieldNames()).map(c -> "s." + c).toArray(String[]::new));
        String updateAssignments = String.join(", ", Arrays.stream(schema.fieldNames()).filter(c -> !pkColumns.contains(c)).map(c -> c + " = s." + c).toArray(String[]::new));

        String joinCondition = sourceReference.getPrimaryKey().getSparkCondition("s", "d");

        StructField[] fields = withMetadataFields(schema).fields();
        // TODO: Remove _timestamp column but keep Op column
        String sourceColumns = String.join(", ", Arrays.stream(fields).map(f -> "? as " + f.name()).toArray(String[]::new));

        dataFrame.foreachPartition(partition -> {
            val partitionStartTime = System.currentTimeMillis();
            logger.debug("Processing partition for Operational Data Store table {}.{}", sourceName, tableName);

            // TODO: manage connections?
            Connection connection = DriverManager.getConnection(url, user, password);


            String targetTableName = format("%s.%s", sourceName, tableName);


            String sql = format("WITH s AS (SELECT %s)\n" +
                            "MERGE INTO %s d\n" +
                            "USING s ON %s\n" +
                            "    WHEN MATCHED AND Op = 'D' THEN DELETE\n" +
                            "    WHEN MATCHED AND Op = 'U' THEN UPDATE SET %s\n" +
                            "WHEN NOT MATCHED AND Op = 'I' THEN INSERT (%s) VALUES (%s)",
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
        if (dataType instanceof StringType) {
            ps.setString(i, row.getAs(field.name()));
        } else if (dataType instanceof IntegerType) {
            ps.setInt(i, row.getAs(field.name()));
        } else if (dataType instanceof LongType) {
            ps.setLong(i, row.getAs(field.name()));
        } else if (dataType instanceof DoubleType) {
            ps.setDouble(i, row.getAs(field.name()));
        } else if (dataType instanceof FloatType) {
            ps.setFloat(i, row.getAs(field.name()));
        } else if (dataType instanceof ShortType) {
            ps.setShort(i, row.getAs(field.name()));
        } else if (dataType instanceof ByteType) {
            ps.setByte(i, row.getAs(field.name()));
        } else if (dataType instanceof BooleanType) {
            ps.setBoolean(i, row.getAs(field.name()));
        } else if (dataType instanceof BinaryType) {
            ps.setBytes(i, row.getAs(field.name()));
        } else if (dataType instanceof TimestampType) {
            ps.setTimestamp(i, row.getAs(field.name()));
        } else if (dataType instanceof DateType) {
            ps.setDate(i, row.getAs(field.name()));
        } else if (dataType instanceof DecimalType) {
            ps.setBigDecimal(i, row.getAs(field.name()));
        } else {
            throw new IllegalStateException(dataType + " is an unrecognized type");
        }
    }
}
