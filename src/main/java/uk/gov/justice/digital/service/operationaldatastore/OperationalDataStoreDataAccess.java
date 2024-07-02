package uk.gov.justice.digital.service.operationaldatastore;

import com.google.common.annotations.VisibleForTesting;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreConnectionDetails;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.OperationalDataStoreException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Responsible for accessing the Operational DataStore.
 */
@Singleton
public class OperationalDataStoreDataAccess {

    private static final Logger logger = LoggerFactory.getLogger(OperationalDataStoreDataAccess.class);

    private final String jdbcUrl;
    // Used by Spark to write to the DataStore
    private final Properties jdbcProps;
    private final DataSource dataSource;

    @Inject
    public OperationalDataStoreDataAccess(
            OperationalDataStoreConnectionDetailsService connectionDetailsService,
            ConnectionPoolProvider connectionPoolProvider
    ) {
        logger.debug("Retrieving connection details for Operational DataStore");
        OperationalDataStoreConnectionDetails connectionDetails = connectionDetailsService.getConnectionDetails();
        jdbcUrl = connectionDetails.getUrl();
        jdbcProps = new Properties();
        jdbcProps.put("driver", connectionDetails.getJdbcDriverClassName());
        jdbcProps.put("user", connectionDetails.getCredentials().getUsername());
        jdbcProps.put("password", connectionDetails.getCredentials().getPassword());
        dataSource = connectionPoolProvider.getConnectionPool(
                jdbcUrl,
                connectionDetails.getJdbcDriverClassName(),
                connectionDetails.getCredentials().getUsername(),
                connectionDetails.getCredentials().getPassword()
        );
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

    void merge(String temporaryTableName, String destinationTableName, SourceReference sourceReference) {
        val startTime = System.currentTimeMillis();
        logger.debug("Merging into destination table {}", destinationTableName);
        String mergeSql = buildMergeSql(temporaryTableName, destinationTableName, sourceReference);
        logger.debug("Merge SQL is {}", mergeSql);
        String truncateSql = format("TRUNCATE TABLE %s", temporaryTableName);
        logger.debug("truncate SQL is {}", truncateSql);

        try (Connection connection = dataSource.getConnection()) {
            try(Statement statement = connection.createStatement()) {
                statement.execute(mergeSql);
                logger.debug("Finished running MERGE into destination table {}", destinationTableName);
                // Truncation of the temporary loading table is not really required since spark will truncate it
                // before it is reloaded - we do it just to keep space free.
                statement.execute(truncateSql);
                logger.debug("Finished running TRUNCATE on temporary table {}", temporaryTableName);
            }
        } catch (SQLException e) {
            throw new OperationalDataStoreException("Exception during merge from temporary table to destination", e);
        }

        logger.debug("Finished merging into destination table {} in {}ms", destinationTableName, System.currentTimeMillis() - startTime);
    }

    @VisibleForTesting
    String buildMergeSql(String temporaryTableName, String destinationTableName, SourceReference sourceReference) {
        // Build the various fragments of the SQL we need
        String[] lowerCaseFieldNames = fieldNamesToLowerCase(sourceReference);
        String joinCondition = buildJoinCondition(sourceReference);
        String updateAssignments = buildUpdateAssignments(sourceReference, lowerCaseFieldNames);
        String insertColumnNames = buildInsertColumnNames(lowerCaseFieldNames);
        String insertValues = buildInsertValues(lowerCaseFieldNames);

        // 'd' is the destination table we merge into.
        // 's' is the source table we merge from.
        return format("MERGE INTO %s d\n" +
                        "USING %s s ON %s\n" +
                        "    WHEN MATCHED AND s.op = 'D' THEN DELETE\n" +
                        "    WHEN MATCHED AND s.op = 'U' THEN UPDATE SET %s\n" +
                        "    WHEN NOT MATCHED AND (s.op = 'I' OR s.op = 'U') THEN INSERT (%s) VALUES (%s)",
                destinationTableName, temporaryTableName, joinCondition, updateAssignments, insertColumnNames, insertValues);
    }

    private String[] fieldNamesToLowerCase(SourceReference sourceReference) {
        return Arrays.stream(sourceReference.getSchema().fieldNames()).map(String::toLowerCase).toArray(String[]::new);
    }

    private String buildJoinCondition(SourceReference sourceReference) {
        return sourceReference.getPrimaryKey().getSparkCondition("s", "d").toLowerCase();
    }

    private String buildUpdateAssignments(SourceReference sourceReference, String[] lowerCaseFieldNames) {
        Set<String> pkColumns = sourceReference.getPrimaryKey().getKeyColumnNames().stream().map(String::toLowerCase).collect(Collectors.toSet());
        return String.join(", ", Arrays.stream(lowerCaseFieldNames).filter(c -> !pkColumns.contains(c)).map(c -> c + " = s." + c).toArray(String[]::new));
    }

    private String buildInsertColumnNames(String[] lowerCaseFieldNames) {
        return String.join(", ", lowerCaseFieldNames);
    }

    private String buildInsertValues(String[] lowerCaseFieldNames) {
        return String.join(", ", Arrays.stream(lowerCaseFieldNames).map(c -> "s." + c).toArray(String[]::new));
    }
}
