package uk.gov.justice.digital.service;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.LoggerFactory;

import java.util.Collections;


public class DomainSchemaService {

    private static final long serialVersionUID = 1L;

    protected final AWSGlue glueClient;

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DomainSchemaService.class);

    public DomainSchemaService(AWSGlue glueClient) {
        this.glueClient = glueClient;
    }

    public boolean databaseExists(String databaseName) {
        GetDatabaseRequest request = new GetDatabaseRequest().withName(databaseName);

        try {
            GetDatabaseResult result = glueClient.getDatabase(request);
            Database db = result.getDatabase();
            if (db != null && db.getName().equals(databaseName)) {
                logger.info("Hive Catalog Database '" + databaseName + "' found");
                return Boolean.TRUE;
            }
        } catch (Exception e) {
            logger.error("Hive Catalog Database check failed :" + e.getMessage());
            return Boolean.FALSE;
        }
        return Boolean.FALSE;
    }

    // This is needed only for unit testing
    public void createDatabase(final String databaseName) {
        // Create a database if it doesn't exist
        DatabaseInput databaseInput = new DatabaseInput().withName(databaseName);
        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest().withDatabaseInput(databaseInput);
        try {
            glueClient.createDatabase(createDatabaseRequest);
        } catch (AlreadyExistsException ae) {
            logger.error(databaseName + "already exists" + ae.getMessage());
        }
    }

    // This is needed only for unit testing
    public void deleteDatabase(final String databaseName, final String catalogId) {
        DeleteDatabaseRequest deleteRequest = new DeleteDatabaseRequest()
                .withCatalogId(catalogId)
                .withName(databaseName);
        try {
            glueClient.deleteDatabase(deleteRequest);
        } catch (Exception e) {
            logger.error("Unable to delete database '" + databaseName + "': " + e.getMessage());
        }
    }

    public boolean tableExists(String databaseName, String tableName) {
        GetTableRequest request = new GetTableRequest()
                .withDatabaseName(databaseName)
                .withName(tableName);
        try {
            glueClient.getTable(request);
            logger.info("Hive Catalog Table '" + tableName + "' found");
            return Boolean.TRUE;
        } catch (EntityNotFoundException e) {
            logger.error("Hive Catalog Table check failed : " + e.getMessage());
            return Boolean.FALSE;
        }
    }

    public void updateTable(final String databaseName, final String tableName, final String path,
                            final Dataset<Row> dataframe) {
        // First delete the table
        deleteTable(databaseName, tableName);
        // then recreate
        createTable(databaseName, tableName, path, dataframe);
    }

    public void deleteTable(final String databaseName, final String tableName) {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
                .withDatabaseName(databaseName)
                .withName(tableName);
        try {
            glueClient.deleteTable(deleteTableRequest);
        } catch (Exception e) {
            logger.error("Delete failed for " + databaseName + ":" + tableName + " " + e.getMessage());
        }

    }

    @SuppressWarnings("serial")
    public void createTable(final String databaseName, final String tableName, final String path,
                            final Dataset<Row> dataframe) {
        // Create a CreateTableRequest
        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withDatabaseName(databaseName)
                .withTableInput(new TableInput()
                        .withName(tableName)
                        .withParameters(new java.util.HashMap<String, String>())
                        .withTableType("EXTERNAL_TABLE")
                        .withParameters(Collections.singletonMap("classification", "parquet"))
                        .withStorageDescriptor(new StorageDescriptor()
                                .withColumns(getColumns(dataframe.schema()))
                                .withLocation(path)
                                .withInputFormat("org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat")
                                .withOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
                                .withSerdeInfo(new SerDeInfo()
                                        .withSerializationLibrary("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
                                        .withParameters(new java.util.HashMap<String, String>() {{
                                            put("serialization.format", ",");
                                        }}))
                                .withCompressed(false)
                                .withNumberOfBuckets(0)
                                .withStoredAsSubDirectories(false)
                        )
                );


        // Create the table in the AWS Glue Data Catalog
        glueClient.createTable(createTableRequest);
    }

    private static java.util.List<Column> getColumns(StructType schema) {
        java.util.List<Column> columns = new java.util.ArrayList<Column>();
        for (StructField field : schema.fields()) {
            columns.add(new Column()
                    .withName(field.name())
                    .withType(field.dataType().typeName())
                    .withComment(""));
        }
        return columns;
    }
}
