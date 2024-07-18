package uk.gov.justice.digital.service.operationaldatastore.dataaccess;

import jakarta.inject.Singleton;
import uk.gov.justice.digital.config.JobArguments;

import javax.sql.DataSource;

@Singleton
public class OperationalDataStoreRepositoryProvider {

    private final JobArguments jobArguments;

    public OperationalDataStoreRepositoryProvider(JobArguments jobArguments) {
        this.jobArguments = jobArguments;
    }

    OperationalDataStoreRepository getOperationalDataStoreRepository(DataSource dataSource) {
       return new OperationalDataStoreRepository(dataSource, jobArguments);
    }
}
