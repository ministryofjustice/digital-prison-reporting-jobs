package uk.gov.justice.digital.service.operationaldatastore;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.SourceReference;

/**
 * Entrypoint for access to the Operational DataStore.
 */
@Singleton
@Requires(property = "dpr.operational.data.store.write.enabled")
public class OperationalDataStoreServiceImpl implements OperationalDataStoreService {

    private static final Logger logger = LoggerFactory.getLogger(OperationalDataStoreServiceImpl.class);

    private final OperationalDataStoreTransformation transformer;
    private final OperationalDataStoreDataAccess operationalDataStoreDataAccess;

    @Inject
    public OperationalDataStoreServiceImpl(
            OperationalDataStoreTransformation transformer,
            OperationalDataStoreDataAccess operationalDataStoreDataAccess
    ) {
        this.transformer = transformer;
        this.operationalDataStoreDataAccess = operationalDataStoreDataAccess;
    }

    public void storeBatchData(Dataset<Row> dataFrame, SourceReference sourceReference) {
        String destinationTableName = sourceReference.getFullyQualifiedTableName();
        val startTime = System.currentTimeMillis();
        logger.info("Processing records for Operational Data Store table {}", destinationTableName);

        Dataset<Row> transformedDf = transformer.transform(dataFrame);
        operationalDataStoreDataAccess.overwriteTable(transformedDf, destinationTableName);

        logger.info("Finished processing records for Operational Data Store table {} in {}ms",
                destinationTableName, System.currentTimeMillis() - startTime);
    }
}
