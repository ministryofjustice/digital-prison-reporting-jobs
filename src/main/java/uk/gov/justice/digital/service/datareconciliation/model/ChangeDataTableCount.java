package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import static uk.gov.justice.digital.service.datareconciliation.ReconciliationTolerance.equalWithTolerance;

/**
 * Change data counts - i.e. counts of insert, update and delete operations.
 */
@Data
@AllArgsConstructor
public class ChangeDataTableCount {

    private final double relativeTolerance;
    private final long absoluteTolerance;

    private long insertCount = 0L;
    private long updateCount = 0L;
    private long deleteCount = 0L;

    public ChangeDataTableCount(double relativeTolerance, long absoluteTolerance) {
        this.relativeTolerance = relativeTolerance;
        this.absoluteTolerance = absoluteTolerance;
    }

    @Override
    public String toString() {
        return "Inserts: " + insertCount + ", Updates: " + updateCount + ", Deletes: " + deleteCount;
    }

    public ChangeDataTableCount combineCounts(ChangeDataTableCount other) {
        if (this.relativeTolerance != other.relativeTolerance || this.absoluteTolerance != other.absoluteTolerance) {
            throw new IllegalArgumentException("Cannot combine counts with different tolerances");
        }
        return new ChangeDataTableCount(
                other.getRelativeTolerance(),
                other.getAbsoluteTolerance(),
                insertCount + other.getInsertCount(),
                updateCount + other.getUpdateCount(),
                deleteCount + other.getDeleteCount()
        );
    }

    // Defines our own notion of exact equality that disregards unequal tolerances
    public boolean countsEqual(ChangeDataTableCount other) {
        if (other == null) {
            return false;
        }
        return this.insertCount == other.insertCount &&
                this.updateCount == other.updateCount &&
                this.deleteCount == other.deleteCount;
    }

    // Defines equality within tolerance, disregarding unequal tolerances, and using this object's tolerances
    public boolean countsEqualWithinTolerance(ChangeDataTableCount other) {
        if (other == null) {
            return false;
        }
        return equalWithTolerance(this.insertCount, other.insertCount, this.absoluteTolerance, this.relativeTolerance) &&
                equalWithTolerance(this.updateCount, other.updateCount, this.absoluteTolerance, this.relativeTolerance) &&
                equalWithTolerance(this.deleteCount, other.deleteCount, this.absoluteTolerance, this.relativeTolerance);
    }
}
