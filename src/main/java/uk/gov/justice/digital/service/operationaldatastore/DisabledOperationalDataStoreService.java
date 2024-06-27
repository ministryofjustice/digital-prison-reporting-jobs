package uk.gov.justice.digital.service.operationaldatastore;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.SourceReference;

@Singleton
@Requires(property = "dpr.operational.data.store.write.enabled", value = "false")
public class DisabledOperationalDataStoreService implements OperationalDataStoreServiceI {

    private static final Logger logger = LoggerFactory.getLogger(DisabledOperationalDataStoreService.class);

    public DisabledOperationalDataStoreService() {
    }

    @Override
    public void storeBatchData(Dataset<Row> dataFrame, SourceReference sourceReference) {
        logger.info("Operational data store functionality is disabled");
    }
}
