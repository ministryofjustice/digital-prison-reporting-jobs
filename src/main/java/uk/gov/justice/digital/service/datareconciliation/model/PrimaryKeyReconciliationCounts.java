package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.val;

import java.util.HashMap;
import java.util.Map;

public class PrimaryKeyReconciliationCounts implements DataReconciliationResult {

    private final Map<String, PrimaryKeyReconciliationCount> counts = new HashMap<>();

    public void put(String fullTableName, PrimaryKeyReconciliationCount tableCount) {
        counts.put(fullTableName, tableCount);
    }

    /**
     * Returns the value to which the specified key is mapped, or
     *  {@code null} if this map contains no mapping for the key
     */
    public PrimaryKeyReconciliationCount get(String fullTableName) {
        return counts.get(fullTableName);
    }

    @Override
    public boolean isSuccess() {
        return counts.values().stream().allMatch(PrimaryKeyReconciliationCount::countsAreZero);
    }

    @Override
    public String summary() {
        StringBuilder sb = new StringBuilder("Primary Key Reconciliation Counts ");
        if (isSuccess()) {
            sb.append("MATCH:\n");
        } else {
            sb.append("DO NOT MATCH:\n");
        }

        for (val entrySet: counts.entrySet()) {
            val tableName = entrySet.getKey();
            val count = entrySet.getValue();
            sb.append("For table ").append(tableName).append(":\n");
            sb.append("\t").append(count.toString()).append("\n");
        }

        return sb.toString();
    }
}
