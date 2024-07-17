package uk.gov.justice.digital.service.operationaldatastore.dataaccess;

import uk.gov.justice.digital.datahub.model.DataHubOperationalDataStoreManagedTable;
import uk.gov.justice.digital.exception.OperationalDataStoreException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class OperationalDataStoreRepository {

    private final DataSource dataSource;

    public OperationalDataStoreRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    Set<DataHubOperationalDataStoreManagedTable> getDataHubOperationalDataStoreManagedTables() {
        // TODO: Managed table name from configuration
        String managedTablesTableName = "configuration.datahub_managed_tables";
        String sql = "SELECT source, table_name FROM " + managedTablesTableName;

        // If our requirements for this class become more complicated we might consider replacing JDBC with an ORM
        Set<DataHubOperationalDataStoreManagedTable> data = new HashSet<>();
        try (Connection connection = dataSource.getConnection()) {
            try(PreparedStatement ps = connection.prepareStatement(sql)) {
                try(ResultSet rs = ps.executeQuery(sql)) {
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
