package uk.gov.justice.digital.service.operationaldatastore;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import uk.gov.justice.digital.datahub.model.SourceReference;

/**
 * Entrypoint for access to the Operational DataStore.
 */
public interface OperationalDataStoreService {
    void overwriteData(Dataset<Row> dataFrame, SourceReference sourceReference);
    void mergeData(Dataset<Row> dataFrame, SourceReference sourceReference);
}
