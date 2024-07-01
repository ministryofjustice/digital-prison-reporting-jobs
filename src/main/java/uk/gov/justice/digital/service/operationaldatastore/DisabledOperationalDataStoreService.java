package uk.gov.justice.digital.service.operationaldatastore;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.SourceReference;

@Singleton
@Requires(missingProperty = "dpr.operational.data.store.write.enabled")
public class DisabledOperationalDataStoreService implements OperationalDataStoreService {
    private static final Logger logger = LoggerFactory.getLogger(DisabledOperationalDataStoreService.class);
    @Override
    public void storeBatchData(Dataset<Row> dataFrame, SourceReference sourceReference) {
        logger.info("Operational DataStore functionality is disabled, skipping table {}.{}", sourceReference.getSource(), sourceReference.getTable());
    }
}
