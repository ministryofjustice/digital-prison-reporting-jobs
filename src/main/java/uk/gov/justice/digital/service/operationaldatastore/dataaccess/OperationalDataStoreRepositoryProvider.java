package uk.gov.justice.digital.service.operationaldatastore.dataaccess;

import jakarta.inject.Singleton;

import javax.sql.DataSource;

@Singleton
public class OperationalDataStoreRepositoryProvider {

    OperationalDataStoreRepository getOperationalDataStoreRepository(DataSource dataSource) {
       return new OperationalDataStoreRepository(dataSource);
    }
}
