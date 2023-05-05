package uk.gov.justice.digital.service;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;

public class SchemaTestHelpers {

    // This is needed only for integration testing
    public static void createDatabase(final AWSGlue glue, final String databaseName) {
        // Create a database if it doesn't exist
        DatabaseInput databaseInput = new DatabaseInput().withName(databaseName);
        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest().withDatabaseInput(databaseInput);
        try {
            glue.createDatabase(createDatabaseRequest);
        } catch (AlreadyExistsException ae) {
            throw new RuntimeException(ae);
        }
    }

    // This is needed only for integration testing
    public static void deleteDatabase(final AWSGlue glue, final String databaseName, final String catalogId) {
        DeleteDatabaseRequest deleteRequest = new DeleteDatabaseRequest()
                .withCatalogId(catalogId)
                .withName(databaseName);
        try {
            glue.deleteDatabase(deleteRequest);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
