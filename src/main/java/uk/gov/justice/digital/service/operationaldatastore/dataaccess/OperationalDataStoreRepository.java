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

    private final DataSource dataSource;
    private final JobArguments jobArguments;

    public OperationalDataStoreRepository(DataSource dataSource, JobArguments jobArguments) {
        this.dataSource = dataSource;
        this.jobArguments = jobArguments;
    }

    Set<DataHubOperationalDataStoreManagedTable> getDataHubOperationalDataStoreManagedTables() {
        // If our requirements for this class become more complicated we might consider replacing JDBC with an ORM
        Set<DataHubOperationalDataStoreManagedTable> data = new HashSet<>();
        try (Connection connection = dataSource.getConnection()) {
            try(Statement s = connection.createStatement()) {
                try(ResultSet rs = s.executeQuery("SELECT source, table_name FROM " + jobArguments.getOperationalDataStoreTablesToWriteTableName())) {
                    while (rs.next()) {
                        String source = rs.getString("source");
                        String tableName = rs.getString("table_name");
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
