package uk.gov.justice.digital.service.operationaldatastore.dataaccess;

import com.google.common.annotations.VisibleForTesting;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.DataHubOperationalDataStoreManagedTable;
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
 * Hub for accessing the Operational DataStore.
 */
@Singleton
public class OperationalDataStoreDataAccess {

    private static final Logger logger = LoggerFactory.getLogger(OperationalDataStoreDataAccess.class);

    private final String jdbcUrl;
    // Used by Spark to access the DataStore
    private final Properties jdbcProps;
    // Used by JDBC to access the DataStore
    private final DataSource dataSource;
    // Maps tables to domain classes and vice-versa
    private final OperationalDataStoreRepository operationalDataStoreRepository;
    // The set DataHub of tables managed by the Operational DataStore. Only these tables should be written to the ODS.
    // Loaded on app startup and refreshed when the app is restarted. This should only ever at maximum be in the order of
    // hundreds and so should not grow too large to stay loaded in memory.
    private final Set<DataHubOperationalDataStoreManagedTable> managedTables;

    @Inject
    public OperationalDataStoreDataAccess(
            OperationalDataStoreConnectionDetailsService connectionDetailsService,
            ConnectionPoolProvider connectionPoolProvider,
            OperationalDataStoreRepositoryProvider operationalDataStoreRepositoryProvider
    ) {
        logger.debug("Retrieving connection details for Operational DataStore");
        OperationalDataStoreConnectionDetails connectionDetails = connectionDetailsService.getConnectionDetails();
        jdbcUrl = connectionDetails.getUrl();
        jdbcProps = connectionDetails.toSparkJdbcProperties();
        dataSource = connectionPoolProvider.getConnectionPool(
                jdbcUrl,
                connectionDetails.getJdbcDriverClassName(),
                connectionDetails.getCredentials().getUsername(),
                connectionDetails.getCredentials().getPassword()
        );
        logger.debug("Finished retrieving connection details for Operational DataStore");
        logger.debug("Retrieving Operational DataStore managed tables");
        operationalDataStoreRepository = operationalDataStoreRepositoryProvider.getOperationalDataStoreRepository(dataSource);
        managedTables = operationalDataStoreRepository.getDataHubOperationalDataStoreManagedTables();
        logger.debug("Finished retrieving Operational DataStore managed tables");
    }

    public void overwriteTable(Dataset<Row> dataframe, String destinationTableName) {
        val startTime = System.currentTimeMillis();
        logger.debug("Writing data to Operational DataStore");
        dataframe.write()
                .mode(SaveMode.Overwrite)
                .jdbc(jdbcUrl, destinationTableName, jdbcProps);
        logger.debug("Finished writing data to Operational DataStore in {}ms", System.currentTimeMillis() - startTime);
    }

    public void merge(String temporaryTableName, String destinationTableName, SourceReference sourceReference) {
        val startTime = System.currentTimeMillis();
        logger.debug("Merging into destination table {}", destinationTableName);
        String mergeSql = buildMergeSql(temporaryTableName, destinationTableName, sourceReference);
        logger.debug("Merge SQL is {}", mergeSql);
        String truncateSql = format("TRUNCATE TABLE %s", temporaryTableName);
        logger.debug("truncate SQL is {}", truncateSql);

        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
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

    public boolean isOperationalDataStoreManagedTable(SourceReference sourceReference) {
        DataHubOperationalDataStoreManagedTable thisTable =
                new DataHubOperationalDataStoreManagedTable(sourceReference.getSource(), sourceReference.getTable());
        return managedTables.contains(thisTable);
    }

    @VisibleForTesting
    String buildMergeSql(String temporaryTableName, String destinationTableName, SourceReference sourceReference) {
        // Build the various fragments of the SQL we need
        String[] lowerCaseFieldNames = fieldNamesToLowerCase(sourceReference);
        String joinCondition = buildJoinCondition(sourceReference);
        String updateAssignments = buildUpdateAssignments(sourceReference, lowerCaseFieldNames);
        String insertColumnNames = buildInsertColumnNames(lowerCaseFieldNames);
        String insertValues = buildInsertValues(lowerCaseFieldNames);

        return "MERGE INTO " + destinationTableName + " destination\n" +
                "USING " + temporaryTableName + " source ON " + joinCondition + "\n" +
                "    WHEN MATCHED AND source.op = 'D' THEN DELETE\n" +
                "    WHEN MATCHED AND source.op = 'U' THEN UPDATE SET " + updateAssignments + "\n" +
                "    WHEN NOT MATCHED AND (source.op = 'I' OR source.op = 'U')" +
                " THEN INSERT (" + insertColumnNames + ") VALUES (" + insertValues + ")";
    }

    private String[] fieldNamesToLowerCase(SourceReference sourceReference) {
        return Arrays.stream(sourceReference.getSchema().fieldNames()).map(String::toLowerCase).toArray(String[]::new);
    }

    private String buildJoinCondition(SourceReference sourceReference) {
        return sourceReference.getPrimaryKey().getSparkCondition("source", "destination").toLowerCase();
    }

    private String buildUpdateAssignments(SourceReference sourceReference, String[] lowerCaseFieldNames) {
        Set<String> pkColumns = sourceReference.getPrimaryKey().getKeyColumnNames().stream().map(String::toLowerCase).collect(Collectors.toSet());
        return String.join(", ", Arrays.stream(lowerCaseFieldNames).filter(c -> !pkColumns.contains(c)).map(c -> c + " = source." + c).toArray(String[]::new));
    }

    private String buildInsertColumnNames(String[] lowerCaseFieldNames) {
        return String.join(", ", lowerCaseFieldNames);
    }

    private String buildInsertValues(String[] lowerCaseFieldNames) {
        return String.join(", ", Arrays.stream(lowerCaseFieldNames).map(c -> "source." + c).toArray(String[]::new));
    }
}
