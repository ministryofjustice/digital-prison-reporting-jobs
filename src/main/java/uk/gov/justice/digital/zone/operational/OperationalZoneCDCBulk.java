package uk.gov.justice.digital.zone.operational;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.zone.Zone;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import static java.lang.String.format;

public class OperationalZoneCDCBulk implements Zone {

    private static final Logger logger = LoggerFactory.getLogger(OperationalZoneCDCBulk.class);
    private static final String LOADING_SCHEMA = "loading";

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

        String temporaryTableName = LOADING_SCHEMA + "." + tableName;
        logger.debug("Loading to temporary table {}", temporaryTableName);
        Properties props = new Properties();
        props.put("user", user);
        props.put("password", password);
        dataFrame.write().mode(SaveMode.Overwrite).jdbc(url, temporaryTableName, props);
        logger.debug("Finished loading to temporary table {}", temporaryTableName);

        StructType schema = sourceReference.getSchema();
        Collection<String> pkColumns = sourceReference.getPrimaryKey().getKeyColumnNames();
        String insertColumnNames = String.join(", ", schema.fieldNames());
        String insertValues = String.join(", ", Arrays.stream(schema.fieldNames()).map(c -> "s." + c).toArray(String[]::new));
        String updateAssignments = String.join(", ", Arrays.stream(schema.fieldNames()).filter(c -> !pkColumns.contains(c)).map(c -> c + " = s." + c).toArray(String[]::new));

        String joinCondition = sourceReference.getPrimaryKey().getSparkCondition("s", "d");

        String destinationTableName = sourceName + "." + tableName;
        logger.debug("Merging into destination table {}", destinationTableName);
        String sql = format("MERGE INTO %s d\n" +
                        "USING %s s ON %s\n" +
                        "    WHEN MATCHED AND s.\"Op\" = 'D' THEN DELETE\n" +
                        "    WHEN MATCHED AND s.\"Op\" = 'U' THEN UPDATE SET %s\n" +
                        "WHEN NOT MATCHED AND s.\"Op\" = 'I' THEN INSERT (%s) VALUES (%s)",
                destinationTableName, temporaryTableName, joinCondition, updateAssignments, insertColumnNames, insertValues);
        logger.debug("SQL is {}", sql);
        // TODO: manage connections?
        try {
            Connection connection = DriverManager.getConnection(url, user, password);
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            logger.error("Exception during merge from temporary table to destination", e);
            throw new RuntimeException(e);
        }


        logger.debug("Finished merging into destination table {}", destinationTableName);

        logger.info("Processed batch for Operational Data Store table {}.{} in {}ms", sourceName, tableName, System.currentTimeMillis() - startTime);
        return dataFrame;
    }
}
