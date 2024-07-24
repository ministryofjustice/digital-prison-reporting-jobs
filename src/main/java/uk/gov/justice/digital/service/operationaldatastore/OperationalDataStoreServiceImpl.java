package uk.gov.justice.digital.service.operationaldatastore;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.OperationalDataStoreException;
import uk.gov.justice.digital.service.operationaldatastore.dataaccess.OperationalDataStoreDataAccess;

import static java.lang.String.format;
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

    private final String loadingSchema;
    private final OperationalDataStoreTransformation transformer;
    private final OperationalDataStoreDataAccess operationalDataStoreDataAccess;

    @Inject
    public OperationalDataStoreServiceImpl(
            JobArguments jobArguments,
            OperationalDataStoreTransformation transformer,
            OperationalDataStoreDataAccess operationalDataStoreDataAccess
    ) {
        this.loadingSchema = jobArguments.getOperationalDataStoreLoadingSchemaName();
        this.transformer = transformer;
        this.operationalDataStoreDataAccess = operationalDataStoreDataAccess;
    }

    @Override
    public void overwriteData(Dataset<Row> dataFrame, SourceReference sourceReference) {
        val startTime = System.currentTimeMillis();
        String destinationTableName = sourceReference.getFullyQualifiedTableName();
        if (operationalDataStoreDataAccess.isOperationalDataStoreManagedTable(sourceReference)) {
            if (operationalDataStoreDataAccess.tableExists(sourceReference)) {
                logger.info("Processing records to write to Operational Data Store table {}", destinationTableName);

                Dataset<Row> transformedDf = transformer
                        .transform(dataFrame)
                        // We don't store these metadata columns in the destination table so we remove them
                        .drop(OPERATION.toLowerCase(), TIMESTAMP.toLowerCase(), CHECKPOINT_COL.toLowerCase());
                operationalDataStoreDataAccess.overwriteTable(transformedDf, destinationTableName);

                logger.info("Finished processing records to write to Operational Data Store table {} in {}ms",
                        destinationTableName, System.currentTimeMillis() - startTime);
            } else {
                // We explicitly don't want Spark to create the table for us to ensure DDL is managed in one place in
                // the migrations in the Transfer Component. If the load is run when someone has forgotten to create
                // the table first then the job will fail and ask them to create the table.
                String msg = format(
                        "Table %s does not exist. Please create it in the Transfer Component before running the load",
                        destinationTableName
                );
                throw new OperationalDataStoreException(msg);
            }
        } else {
            logger.info("Skipping write to Operational Data Store for non-managed table {}", destinationTableName);
        }
    }

    @Override
    public void mergeData(Dataset<Row> dataFrame, SourceReference sourceReference) {
        val startTime = System.currentTimeMillis();
        String destinationTableName = sourceReference.getFullyQualifiedTableName();
        if (operationalDataStoreDataAccess.isOperationalDataStoreManagedTable(sourceReference)) {
            logger.info("Processing records to merge into Operational Data Store table {}", destinationTableName);

            String temporaryLoadingTableName = loadingSchema + "." + sourceReference.getTable();
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
        } else {
            logger.info("Skipping merge to Operational Data Store for non-managed table {}", destinationTableName);
        }
    }
}
