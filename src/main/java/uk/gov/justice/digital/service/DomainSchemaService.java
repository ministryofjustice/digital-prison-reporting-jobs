package uk.gov.justice.digital.service;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.glue.JobClient;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;

@Singleton
public class DomainSchemaService {

    private static final long serialVersionUID = 1L;

    protected final AWSGlue glueClient;

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DomainSchemaService.class);

    @Inject
    public DomainSchemaService(JobClient jobClient) {
        this(jobClient.getGlueClient());
    }

    protected DomainSchemaService(AWSGlue client) {
        this.glueClient = client;
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
            logger.error(e.getMessage());
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
                                .withLocation(path + "/_symlink_format_manifest")
                                .withInputFormat("org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat")
                                .withOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
                                .withSerdeInfo(new SerDeInfo()
                                        .withSerializationLibrary("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                                        .withParameters(new java.util.HashMap<String, String>() {{
                                            put("serialization.format", ",");
                                        }}))
                                .withCompressed(false)
                                .withNumberOfBuckets(0)
                                .withStoredAsSubDirectories(false)
                        )
                );


        // Create the table in the AWS Glue Data Catalog
        try {
            glueClient.createTable(createTableRequest);
            logger.info("Table created in hive : " + tableName);
        } catch (Exception e) {
            logger.error("Create table failed : Table Name " + tableName + "Reason: " + e.getMessage());
        }
    }

    private static java.util.List<Column> getColumns(StructType schema) {
        java.util.List<Column> columns = new java.util.ArrayList<Column>();
        for (StructField field : schema.fields()) {
            Column col = new Column()
                    .withName(field.name())
                    .withType(field.dataType().typeName())
                    .withComment("");
            // Null type not supported in AWS Glue Catalog
            // numerical types mapping should be explicit and not automatically picks
            if (col.getType().equals("long")) {
                col.setType("bigint");
            } else if (col.getType().equals("short")) {
                col.setType("smallint");
            } else if (col.getType().equals("integer")) {
                col.setType("int");
            } else if (col.getType().equals("byte")) {
                col.setType("tinyint");
            }
            columns.add(col);
        }
        return columns;
    }
}
