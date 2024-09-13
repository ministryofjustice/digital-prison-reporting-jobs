package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.Data;

@Data
public class ChangeDataTotalCounts {

    private final CountsByTable<ChangeDataTableRawZoneCount> rawZoneCounts;
    private final CountsByTable<ChangeDataTableDmsCount> dmsCounts;

    public boolean isFailure() {
        boolean differentTables = !rawZoneCounts.keySet().equals(dmsCounts.keySet());
        boolean countsByOperationDiffer = rawZoneCounts.entrySet().stream().anyMatch(entry -> {
            // todo handle nulls
            String tableName = entry.getKey();
            ChangeDataTableRawZoneCount rawCount = entry.getValue();
            ChangeDataTableDmsCount dmsCount = dmsCounts.get(tableName);
            return !rawCount.dmsCountsMatch(dmsCount);
        });
        return differentTables || countsByOperationDiffer;
    }



    public String summary() {
        StringBuilder sb = new StringBuilder("Change Data Total Counts ");
        if (isFailure()) {
            sb.append("DO NOT MATCH:\n");
        } else {
            sb.append("MATCH:\n");
        }

        rawZoneCounts.entrySet().forEach(entry -> {
            // todo handle nulls
            String tableName = entry.getKey();
            ChangeDataTableRawZoneCount rawCount = entry.getValue();
            ChangeDataTableDmsCount dmsCount = dmsCounts.get(tableName);

            sb.append("For table ").append(tableName);
            if (rawCount.dmsCountsMatch(dmsCount)) {
                sb.append(" MATCH:\n");
            } else {
                sb.append(" DOES NOT MATCH:\n");
            }

            sb.append("\t").append("Inserts - ");
            addSummaryRow(sb, rawCount.getInsertCount(), dmsCount.getInsertCount(), dmsCount.getAppliedInsertCount());
            sb.append("\t").append("Updates - ");
            addSummaryRow(sb, rawCount.getUpdateCount(), dmsCount.getUpdateCount(), dmsCount.getAppliedUpdateCount());
            sb.append("\t").append("Deletes - ");
            addSummaryRow(sb, rawCount.getDeleteCount(), dmsCount.getDeleteCount(), dmsCount.getAppliedDeleteCount());
        });
        return sb.toString();
    }

    private void addSummaryRow(StringBuilder sb, long rawCount, long dmsCount, long appliedDmsCount) {
        sb.append("Raw: ");
        sb.append(rawCount);
        sb.append(", DMS: ");
        sb.append(dmsCount);
        sb.append(", DMS applied: ");
        sb.append(appliedDmsCount);
        sb.append("\n");
    }
}
