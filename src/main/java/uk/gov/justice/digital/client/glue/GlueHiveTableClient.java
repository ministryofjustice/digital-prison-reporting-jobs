package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.*;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Singleton
public class GlueHiveTableClient {

    private static final Logger logger = LoggerFactory.getLogger(GlueHiveTableClient.class);

    public static final String MAPRED_PARQUET_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    public static final String SYMLINK_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat";

    private final AWSGlue glueClient;

    @Inject
    public GlueHiveTableClient(GlueClientProvider glueClientProvider) {
        this.glueClient = glueClientProvider.getClient();
    }

    public void createParquetTable(String database, String table, String dataPath, StructType schema) throws AWSGlueException {
        val storageDescriptor = createStorageDescriptor(dataPath, MAPRED_PARQUET_INPUT_FORMAT, schema);
        createTable(database, table, storageDescriptor);
    }

    public void createTableWithSymlink(String database, String table, String dataPath, StructType schema) throws AWSGlueException {
        String location = dataPath + "/_symlink_format_manifest";
        val storageDescriptor = createStorageDescriptor(location, SYMLINK_INPUT_FORMAT, schema);
        createTable(database, table, storageDescriptor);
    }

    public void deleteTable(String database, String table) throws AWSGlueException {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
                .withDatabaseName(database)
                .withName(table);

        try {
            logger.info("Deleting table {}.{}", database, table);
            glueClient.deleteTable(deleteTableRequest);
            logger.info("Successfully deleted table {}.{}", database, table);
        } catch (EntityNotFoundException e) {
            logger.info("Did not delete non-existent table {}.{}", database, table);
        }
    }

    private void createTable(String database, String table, StorageDescriptor storageDescriptor) {
        CreateTableRequest createTableRequest = getCreateTableRequest(database, table, storageDescriptor);

        logger.info("Creating table {}.{}", database, table);
        glueClient.createTable(createTableRequest);
        logger.info("Successfully created table {}.{}", database, table);
    }

    private StorageDescriptor createStorageDescriptor(String location, String inputFormat, StructType schema) {
        return new StorageDescriptor()
                .withColumns(getColumnsAndModifyTypes(schema))
                .withLocation(location)
                .withInputFormat(inputFormat)
                .withOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
                .withSerdeInfo(
                        new SerDeInfo()
                                .withSerializationLibrary("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                                .withParameters(Collections.singletonMap("serialization.format", ","))
                )
                .withCompressed(false)
                .withNumberOfBuckets(0)
                .withStoredAsSubDirectories(false);
    }

    private CreateTableRequest getCreateTableRequest(String database, String table, StorageDescriptor storageDescriptor) {
        return new CreateTableRequest()
                .withDatabaseName(database)
                .withTableInput(new TableInput()
                        .withName(table)
                        .withTableType("EXTERNAL_TABLE")
                        .withParameters(Collections.singletonMap("classification", "parquet"))
                        .withStorageDescriptor(storageDescriptor)
                );
    }

    private List<Column> getColumnsAndModifyTypes(StructType schema) {
        val columns = new ArrayList<Column>();
        for (StructField field : schema.fields()) {
            val column = new Column().withName(field.name()).withType(field.dataType().typeName());
            // Null type not supported in AWS Glue Catalog.
            // Numerical type mappings should be explicit and not automatically selected.
            switch (column.getType()) {
                case "long": column.setType("bigint");
                    break;
                case "short": column.setType("smallint");
                    break;
                case "integer": column.setType("int");
                    break;
                case "byte": column.setType("tinyint");
                    break;
                default:
                    break;
            }

            columns.add(column);
        }
        return columns;
    }
}
