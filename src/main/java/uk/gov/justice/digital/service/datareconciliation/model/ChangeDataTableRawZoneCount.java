package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ChangeDataTableRawZoneCount {
    private long insertCount = 0;
    private long updateCount = 0;
    private long deleteCount = 0;

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

    public ChangeDataTableRawZoneCount combineCounts(ChangeDataTableRawZoneCount otherCounts) {
        long combinedInsertCount = insertCount + otherCounts.getInsertCount();
        long combinedUpdateCount = updateCount + otherCounts.getUpdateCount();
        long combinedDeleteCount = deleteCount + otherCounts.getDeleteCount();
        return new ChangeDataTableRawZoneCount(
                combinedInsertCount, combinedUpdateCount, combinedDeleteCount
        );
    }
}
