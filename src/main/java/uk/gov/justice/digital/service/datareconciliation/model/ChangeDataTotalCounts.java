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
        StringBuilder sb = new StringBuilder("Change Data Total Counts:\n");

        rawZoneCounts.entrySet().forEach(entry -> {
            // todo handle nulls
            String tableName = entry.getKey();
            ChangeDataTableRawZoneCount rawCount = entry.getValue();
            ChangeDataTableDmsCount dmsCount = dmsCounts.get(tableName);

            sb.append("For table ").append(tableName).append(":\n");

            sb.append("Inserts - ");
            sb.append("Raw: ");
            sb.append(rawCount.getInsertCount());
            sb.append(", DMS read: ");
            sb.append(dmsCount.getInsertCount());
            sb.append(", DMS applied: ");
            sb.append(dmsCount.getAppliedInsertCount());
            sb.append("\n");

            sb.append("Updates - ");
            sb.append("Raw: ");
            sb.append(rawCount.getUpdateCount());
            sb.append(", DMS read: ");
            sb.append(dmsCount.getUpdateCount());
            sb.append(", DMS applied: ");
            sb.append(dmsCount.getAppliedUpdateCount());
            sb.append("\n");

            sb.append("Deletes - ");
            sb.append("Raw: ");
            sb.append(rawCount.getDeleteCount());
            sb.append(", DMS read: ");
            sb.append(dmsCount.getDeleteCount());
            sb.append(", DMS applied: ");
            sb.append(dmsCount.getAppliedDeleteCount());
            sb.append("\n");
        });
        return sb.toString();
    }
}
