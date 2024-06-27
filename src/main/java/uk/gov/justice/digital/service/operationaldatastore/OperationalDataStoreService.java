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

/**
 * Entrypoint for access to the Operational DataStore.
 */
@Singleton
@Requires(property = "dpr.operational.data.store.write.enabled", value = "true")
public class OperationalDataStoreService implements OperationalDataStoreServiceI {

    private static final Logger logger = LoggerFactory.getLogger(OperationalDataStoreService.class);

    private final JobArguments jobArguments;
    private final OperationalDataStoreTransformation transformer;
    private final OperationalDataStoreDataAccess operationalDataStoreDataAccess;

    @Inject
    public OperationalDataStoreService(
            JobArguments jobArguments,
            OperationalDataStoreTransformation transformer,
            OperationalDataStoreDataAccess operationalDataStoreDataAccess
    ) {
        this.jobArguments = jobArguments;
        this.transformer = transformer;
        this.operationalDataStoreDataAccess = operationalDataStoreDataAccess;
    }

    public void storeBatchData(Dataset<Row> dataFrame, SourceReference sourceReference) {
        String destinationTableName = sourceReference.getFullyQualifiedTableName();
        if(jobArguments.isOperationalDataStoreWriteEnabled()) {
            val startTime = System.currentTimeMillis();
            logger.info("Processing records for Operational Data Store table {}", destinationTableName);

            Dataset<Row> transformedDf = transformer.transform(dataFrame);
            operationalDataStoreDataAccess.overwriteTable(transformedDf, destinationTableName);

            logger.info("Finished processing records for Operational Data Store table {} in {}ms",
                    destinationTableName, System.currentTimeMillis() - startTime);
        } else {
            logger.info("Operational Data Store write is disabled so skipping write for table {}", destinationTableName);
        }
    }
}
