package uk.gov.justice.digital.service.operationaldatastore.dataaccess;

import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.DataHubOperationalDataStoreManagedTable;
import uk.gov.justice.digital.exception.OperationalDataStoreException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public class OperationalDataStoreRepository {

    private static final String SOURCE_COL_NAME = "source";
    private static final String TABLE_NAME_COL_NAME = "table_name";

    private final DataSource dataSource;
    private final JobArguments jobArguments;

    public OperationalDataStoreRepository(DataSource dataSource, JobArguments jobArguments) {
        this.dataSource = dataSource;
        this.jobArguments = jobArguments;
    }

    @SuppressWarnings("java:S2077")
    Set<DataHubOperationalDataStoreManagedTable> getDataHubOperationalDataStoreManagedTables() {
        Set<DataHubOperationalDataStoreManagedTable> data = new HashSet<>();
        try (Connection connection = dataSource.getConnection()) {
            try (Statement s = connection.createStatement()) {
                try (ResultSet rs = s.executeQuery("SELECT source, table_name FROM " + jobArguments.getOperationalDataStoreTablesToWriteTableName())) {
                    while (rs.next()) {
                        String source = rs.getString(SOURCE_COL_NAME);
                        String tableName = rs.getString(TABLE_NAME_COL_NAME);
                        data.add(new DataHubOperationalDataStoreManagedTable(source, tableName));
                    }
                    return data;
                }
            }
        } catch (SQLException e) {
            throw new OperationalDataStoreException("Exception when retrieving data for Operational DataStore DataHub Managed Tables", e);
        }
    }
}
