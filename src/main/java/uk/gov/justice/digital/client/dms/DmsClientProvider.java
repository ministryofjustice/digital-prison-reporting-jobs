package uk.gov.justice.digital.client.dms;

import com.amazonaws.services.databasemigrationservice.AWSDatabaseMigrationService;
import com.amazonaws.services.databasemigrationservice.AWSDatabaseMigrationServiceClientBuilder;
import jakarta.inject.Singleton;
import uk.gov.justice.digital.client.ClientProvider;

@Singleton
public class DmsClientProvider implements ClientProvider<AWSDatabaseMigrationService> {

    @Override
    public AWSDatabaseMigrationService getClient() {
        return AWSDatabaseMigrationServiceClientBuilder.defaultClient();
    }

}
