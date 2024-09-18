package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Change data counts - i.e. counts of insert, update and delete operations.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChangeDataTableCount {
    private long insertCount = 0L;
    private long updateCount = 0L;
    private long deleteCount = 0L;

    @Override
    public String toString() {
        return "Inserts: " + insertCount + ", Updates: " + updateCount + ", Deletes: " + deleteCount;
    }

    public ChangeDataTableCount combineCounts(ChangeDataTableCount other) {
        return new ChangeDataTableCount(
                insertCount + other.getInsertCount(),
                updateCount + other.getUpdateCount(),
                deleteCount + other.getDeleteCount()
        );
    }
}
