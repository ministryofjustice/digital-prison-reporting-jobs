package uk.gov.justice.digital.service;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.*;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.LoggerFactory;


public class DomainSchemaService {

    protected final AWSGlue glueClient;

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DomainSchemaService.class);

//    protected static SparkSession spark = null;
//
//    public boolean check_db_exists() {
//        return false;
//    }
//
    public DomainSchemaService(AWSGlue glueClient) {
        this.glueClient = glueClient;
    }
    public void createDatabase(final String databaseName) {
        // Create a database if it doesn't exist
        DatabaseInput databaseInput = new DatabaseInput().withName(databaseName);
        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest().withDatabaseInput(databaseInput);
        try {
            glueClient.createDatabase(createDatabaseRequest);
        } catch (AlreadyExistsException ae) {
            logger.error(databaseName + "already exists");
        }
    }

    public void createTable(final String databaseName, final String tableName, final String path,
                            final Dataset<Row> dataframe) {
        // Create a CreateTableRequest
        CreateTableRequest createTableRequest = new CreateTableRequest()
                .withDatabaseName(databaseName)
                .withTableInput(new TableInput()
                        .withName(tableName)
                        .withParameters(new java.util.HashMap<String, String>())
                        .withTableType("EXTERNAL_TABLE")
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

//
//
//    public boolean delete_db() {
//        return false;
//    }
//
//    public boolean check_db_exists(final String db_name) {
//        return false;
//    }
//
//    public boolean create_tbl(AWSGlue catalog, final Dataset<Row> dataframe, final String schema_name, final String tbl_name) {
//        // Generate schema
//        dataframe.createOrReplaceTempView("temp_" + tbl_name);
////        SparkSession spark = dataframe.sparkSession();
//        spark.sql("drop table if exists " + schema_name + "." + tbl_name);
//        create_db(schema_name);
//        spark.sql("create table " + schema_name + "." + tbl_name
//                + " as select * from temp_" + tbl_name);
//        Dataset<Row> df = spark.sql("show create table " + schema_name + "." + tbl_name);
////        System.out.println(dataframe.schema().toDDL());
//        String createStmt = df.collectAsList().get(0).mkString();
//        System.out.println(createStmt);
//        // Apply to Hive Metastore
//
//        return true;
//    }
//
//    public boolean delete_tbl() {
//        return false;
//    }
//
//    public boolean alter_tbl() {
//        return false;
//    }
//
//    public boolean check_tbl_exists() {
//        return false;
//    }

}
