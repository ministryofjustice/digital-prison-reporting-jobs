package uk.gov.justice.digital.service.operationaldatastore;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import uk.gov.justice.digital.datahub.model.SourceReference;

public interface OperationalDataStoreServiceI {
    void storeBatchData(Dataset<Row> dataFrame, SourceReference sourceReference);
}
