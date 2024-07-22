package uk.gov.justice.digital.service.operationaldatastore.dataaccess;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.DataHubOperationalDataStoreManagedTable;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreConnectionDetails;
import uk.gov.justice.digital.provider.SparkSessionProvider;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

@Singleton
public class OperationalDataStoreRepository {
    private static final String SOURCE_COL_NAME = "source";
    private static final String TABLE_NAME_COL_NAME = "table_name";

    private final JobArguments jobArguments;
    private final SparkSession sparkSession;
    private final String jdbcUrl;
    private final Properties jdbcProps;

    @Inject
    public OperationalDataStoreRepository(
            JobArguments jobArguments,
            OperationalDataStoreConnectionDetailsService connectionDetailsService,
            SparkSessionProvider sparkSessionProvider
    ) {
        this.jobArguments = jobArguments;
        this.sparkSession = sparkSessionProvider.getConfiguredSparkSession(jobArguments);
        OperationalDataStoreConnectionDetails connectionDetails = connectionDetailsService.getConnectionDetails();
        jdbcUrl = connectionDetails.getUrl();
        jdbcProps = connectionDetails.toSparkJdbcProperties();
    }

    Set<DataHubOperationalDataStoreManagedTable> getDataHubOperationalDataStoreManagedTables() {
        Dataset<Row> managedTablesDf = sparkSession.read().jdbc(
                jdbcUrl,
                jobArguments.getOperationalDataStoreTablesToWriteTableName(),
                jdbcProps
        );
        List<DataHubOperationalDataStoreManagedTable> contents = managedTablesDf
                .map((MapFunction<Row, DataHubOperationalDataStoreManagedTable>) row -> {
                    String source = row.getAs(SOURCE_COL_NAME);
                    String table = row.getAs(TABLE_NAME_COL_NAME);
                    return new DataHubOperationalDataStoreManagedTable(source, table);
                }, Encoders.kryo(DataHubOperationalDataStoreManagedTable.class))
                .collectAsList();
        return new HashSet<>(contents);
    }
}
