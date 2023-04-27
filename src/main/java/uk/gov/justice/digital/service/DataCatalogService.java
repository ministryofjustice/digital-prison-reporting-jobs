package uk.gov.justice.digital.service;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.*;
import io.micronaut.context.annotation.Bean;
import jakarta.inject.Inject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import uk.gov.justice.digital.config.JobParameters;

@Bean
public class DataCatalogService {

    private final AWSGlue glueClient;

//    static {
//        glue = AWSGlueClientBuilder.defaultClient();
//    }


    @Inject
    public DataCatalogService(JobParameters jobParameters) {
        final String region = jobParameters.getAwsRegion();
        final AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder
                .EndpointConfiguration("https://glue." + region + ".amazonaws.com", region);
        glueClient = AWSGlueClientBuilder.standard()
                .withEndpointConfiguration(endpointConfiguration)
//                .withCredentials(credentialsProvider)
                .build();
    }


    public void connect() {
        final String connectionName = "my-glue-connection";
        final GetConnectionRequest getConnectionRequest = new GetConnectionRequest()
                .withName(connectionName);

        final GetConnectionResult getConnectionResult = glueClient.getConnection(getConnectionRequest);
        final Connection connection = getConnectionResult.getConnection();

        // Use the connection object to interact with the database or other resource
        System.out.println("Connection name: " + connection.getName());
        System.out.println("Connection type: " + connection.getConnectionType());
        System.out.println("Connection URL: " + connection.getConnectionProperties().get("JDBC_CONNECTION_URL"));
        System.out.println("Connection username: " + connection.getConnectionProperties().get("USERNAME"));
    }


    public void createDatabase(final String db_name) {
        // Create a database if it doesn't exist
        DatabaseInput databaseInput = new DatabaseInput().withName(db_name);
        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest().withDatabaseInput(databaseInput);
        glueClient.createDatabase(createDatabaseRequest);
    }

    public String createDDL(final Dataset<Row> dataframe) {
        dataframe.printSchema();
        return null;
    }
    public void createTable(final String db_name, final String tbl_name, final String ddl) {
        // Create a new table in the database
        String tableLocation = "s3://my-bucket/my-table/";

//        // Set up DataFrame DDL
//        String ddl = "CREATE TABLE myDatabase.myTable (\n"
//                + "col1 string,\n"
//                + "col2 int\n"
//                + ")\n"
//                + "STORED AS PARQUET\n"
//                + "LOCATION 's3://my-bucket/my-folder/'";

        // Set up CreateSchemaRequest
        CreateSchemaRequest createSchemaRequest = new CreateSchemaRequest()
                .withCompatibility("NONE")
                .withSchemaDefinition(ddl);

        glueClient.createSchema(createSchemaRequest);
    }


//    public void execute() {
//        // Create a new instance of the AWSCatalogMetastoreClient
//
//
////                glue.createDatabase()
////                .withCredentials(new AWSStaticCredentialsProvider(credentials))
////                .withRegion(Regions.US_EAST_1)
////                .build();
//
//        // Create a new database in the data catalog
//        CreateDatabaseRequest request = new CreateDatabaseRequest()
//                .withDatabaseInput(new DatabaseInput()
//                        .withName("my_database")
//                        .withDescription("This is my test database"));
//        CreateDatabaseResult result = client.createDatabase(request);
//
//        // Print the ARN of the new database
//        System.out.println("Created database with ARN: " + result.getDatabase().getArn());
//
//    }

    protected void finalize() {
            // Shutdown the client
        glueClient.shutdown();
    }
}
