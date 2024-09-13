package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ChangeDataTableRawZoneCount {
    private long insertCount = 0;
    private long updateCount = 0;
    private long deleteCount = 0;

    public ChangeDataTableRawZoneCount() {
    }

    public String summary() {
        return "Inserts: " + insertCount + ", Updates: " + updateCount + ", Deletes: " + deleteCount;
    }

    public boolean dmsCountsMatch(ChangeDataTableDmsCount dmsCount) {
        return insertCount == dmsCount.getInsertCount() &&
                updateCount == dmsCount.getUpdateCount() &&
                deleteCount == dmsCount.getDeleteCount() &&
                insertCount == dmsCount.getAppliedInsertCount() &&
                updateCount == dmsCount.getAppliedUpdateCount() &&
                deleteCount == dmsCount.getAppliedDeleteCount();
    }
}
