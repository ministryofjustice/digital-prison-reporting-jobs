package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.Data;

@Data
public class ChangeDataTableDmsCount {
    // The counts that DMS thinks it has read from the source
    private final long insertCount;
    private final long updateCount;
    private final long deleteCount;
    // The counts that DMS thinks it has applied to the target
    private final long appliedInsertCount;
    private final long appliedUpdateCount;
    private final long appliedDeleteCount;
}
