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

import static uk.gov.justice.digital.common.CommonDataFields.CHECKPOINT_COL;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.TIMESTAMP;

/**
 * Entrypoint for access to the Operational DataStore.
 */
@Singleton
@Requires(property = "dpr.operational.data.store.write.enabled")
public class OperationalDataStoreServiceImpl implements OperationalDataStoreService {

    private static final Logger logger = LoggerFactory.getLogger(OperationalDataStoreServiceImpl.class);
    private static final String LOADING_SCHEMA = "loading";

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

    @Override
    public void overwriteData(Dataset<Row> dataFrame, SourceReference sourceReference) {
        val startTime = System.currentTimeMillis();
        String destinationTableName = sourceReference.getFullyQualifiedTableName();
        logger.info("Processing records to write to Operational Data Store table {}", destinationTableName);

        Dataset<Row> transformedDf = transformer
                .transform(dataFrame)
                // We don't store these metadata columns in the destination table so we remove them
                .drop(OPERATION.toLowerCase(), TIMESTAMP.toLowerCase(), CHECKPOINT_COL.toLowerCase());
        operationalDataStoreDataAccess.overwriteTable(transformedDf, destinationTableName);

        logger.info("Finished processing records to write to Operational Data Store table {} in {}ms",
                destinationTableName, System.currentTimeMillis() - startTime);
    }

    @Override
    public void mergeData(Dataset<Row> dataFrame, SourceReference sourceReference) {
        val startTime = System.currentTimeMillis();
        String destinationTableName = sourceReference.getFullyQualifiedTableName();
        logger.info("Processing records to merge into Operational Data Store table {}", destinationTableName);

        String temporaryLoadingTableName = LOADING_SCHEMA + "." + sourceReference.getTable();
        logger.debug("Loading to temporary table {}", temporaryLoadingTableName);

        Dataset<Row> transformedDf = transformer
                .transform(dataFrame)
                // We don't store these metadata columns in the loading temporary table, so we remove them.
                // However, we need the op column for the merge.
                .drop(TIMESTAMP.toLowerCase(), CHECKPOINT_COL.toLowerCase());

        // Load the data to the temporary loading table
        operationalDataStoreDataAccess.overwriteTable(transformedDf, temporaryLoadingTableName);
        logger.debug("Finished loading to temporary table {}", temporaryLoadingTableName);
        logger.debug("Merging to destination table {}", destinationTableName);
        operationalDataStoreDataAccess.merge(temporaryLoadingTableName, destinationTableName, sourceReference);
        logger.debug("Finished merging to destination table {}", destinationTableName);
        logger.info("Finished processing records to merge into Operational Data Store table {} in {}ms",
                destinationTableName, System.currentTimeMillis() - startTime);
    }
}
