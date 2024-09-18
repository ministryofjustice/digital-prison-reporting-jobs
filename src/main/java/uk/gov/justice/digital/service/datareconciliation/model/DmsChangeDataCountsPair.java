package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.Data;

import java.util.Map;

/**
 * Used as a container to return two Maps from table name to ChangeDataTableCount for the DMS.
 * One is the counts for the data DMS thinks it has read and the other is the counts for the
 * data that DMS thinks it has applied.
 */
@Data
public class DmsChangeDataCountsPair {
    // The counts the DMS thinks it has read from the source data store
    private final Map<String, ChangeDataTableCount> dmsChangeDataCounts;
    // The counts the DMS thinks it has applied to the target data store
    private final Map<String, ChangeDataTableCount> dmsAppliedChangeDataCounts;
}
