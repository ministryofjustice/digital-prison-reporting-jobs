package uk.gov.justice.digital.service.operationaldatastore;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.SourceReference;

@Singleton
public class OperationalDataStoreService {

    private static final Logger logger = LoggerFactory.getLogger(OperationalDataStoreService.class);

    private final OperationalDataStoreDataTransformation transformer;
    private final OperationalDataStoreDataAccess operationalDataStoreDataAccess;

    @Inject
    public OperationalDataStoreService(
            OperationalDataStoreDataTransformation transformer,
            OperationalDataStoreDataAccess operationalDataStoreDataAccess
    ) {
        this.transformer = transformer;
        this.operationalDataStoreDataAccess = operationalDataStoreDataAccess;
    }

    public void storeBatchData(Dataset<Row> dataFrame, SourceReference sourceReference) {
        String destinationTableName = sourceReference.getFullyQualifiedTableName();
        logger.info("Processing records for Operational Data Store table {}", destinationTableName);

        // We pass in the source reference schema fields rather than use the dataframe schema because the source reference
        // is the canonical schema and in case are extra fields we don't want due to previous transformations, etc.
        Dataset<Row> transformedDf = transformer.transform(dataFrame, sourceReference.getSchema().fields());
        operationalDataStoreDataAccess.overwriteTable(transformedDf, destinationTableName);
    }
}
