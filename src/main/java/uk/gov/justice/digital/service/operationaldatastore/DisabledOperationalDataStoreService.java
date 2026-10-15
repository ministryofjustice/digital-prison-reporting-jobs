package uk.gov.justice.digital.service.operationaldatastore;

import io.micronaut.context.annotation.Requires;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Singleton;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.SourceReference;

import static uk.gov.justice.digital.config.JobArguments.OPERATIONAL_DATA_STORE_WRITE_ENABLED;

@Singleton
@Requires(property = OPERATIONAL_DATA_STORE_WRITE_ENABLED, notEquals = StringUtils.TRUE)
public class DisabledOperationalDataStoreService implements OperationalDataStoreService {
    private static final Logger logger = LoggerFactory.getLogger(DisabledOperationalDataStoreService.class);

    public DisabledOperationalDataStoreService() {
        logger.info("Operational DataStore functionality is disabled");
    }

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public void overwriteData(Dataset<Row> dataFrame, SourceReference sourceReference) {
        logger.info("Operational DataStore functionality is disabled, Skipping overwrite data to table {}.{}", sourceReference.getSource(), sourceReference.getTable());
    }

    @Override
    public void mergeData(Dataset<Row> dataFrame, SourceReference sourceReference) {
        logger.info("Operational DataStore functionality is disabled, Skipping merge into table {}.{}", sourceReference.getSource(), sourceReference.getTable());
    }

    @Override
    public boolean isOperationalDataStoreManagedTable(SourceReference sourceReference) {
        logger.info("Operational DataStore functionality is disabled, marking table {}.{} as unmanaged", sourceReference.getSource(), sourceReference.getTable());
        return false;
    }

    @Override
    public long getTableRowCount(String tableName) {
        throw new IllegalStateException("Operational DataStore functionality is disabled");
    }
}
