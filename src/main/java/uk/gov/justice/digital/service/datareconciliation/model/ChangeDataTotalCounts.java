package uk.gov.justice.digital.service.datareconciliation.model;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import uk.gov.justice.digital.common.CommonDataFields;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.amazonaws.services.cloudwatch.model.StandardUnit.Count;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChangeDataTotalCounts implements DataReconciliationResult {

    private static final String MISSING_COUNTS_MESSAGE = "MISSING COUNTS";
    private static final String COUNT_METRIC_NAME = "ChangeDataCount";
    private static final String IN_RAW_NOT_DMS_METRIC_NAME = "RawTablesMissingInDMS";
    private static final String IN_DMS_NOT_RAW_METRIC_NAME = "DMSTablesMissingInRaw";

    private Map<String, ChangeDataTableCount> rawZoneCounts;
    private Map<String, ChangeDataTableCount> dmsCounts;
    private Map<String, ChangeDataTableCount> dmsAppliedCounts;

    @Override
    public boolean isSuccess() {
        return sameTables() && rawCountsMatchDmsCounts();
    }

    @Override
    public String summary() {
        StringBuilder sb = new StringBuilder("Change Data Total Counts ");
        if (isSuccess()) {
            sb.append("MATCH:\n\n");
        } else {
            sb.append("DO NOT MATCH:\n\n");
        }

        if (!sameTables()) {
            sb.append("\nThe set of tables for DMS vs Raw zone DO NOT MATCH\n\n");
            sb.append("DMS Tables missing in Raw: ").append(tablesInDmsMissingFromRaw()).append("\n");
            sb.append("Raw Zone/Raw Archive Tables missing in DMS: ").append(tablesInRawMissingFromDms()).append("\n");
            sb.append("\n");
        }

        dmsCounts.forEach((tableName, dmsCount) -> {
            ChangeDataTableCount dmsAppliedCount = dmsAppliedCounts.get(tableName);
            ChangeDataTableCount rawCount = rawZoneCounts.get(tableName);
            sb.append("For table ").append(tableName);
            if (dmsCount != null && dmsCount.equals(dmsAppliedCount) && dmsCount.equals(rawCount)) {
                sb.append(" MATCH:\n");
            } else {
                sb.append(" DOES NOT MATCH:\n");
            }

            sb.append("\t")
                    .append(nullSafeCountString(rawCount))
                    .append("\t")
                    .append(" - Raw")
                    .append("\n");
            sb.append("\t")
                    .append(nullSafeCountString(dmsCount))
                    .append("\t")
                    .append(" - DMS")
                    .append("\n");
            sb.append("\t")
                    .append(nullSafeCountString(dmsAppliedCount))
                    .append("\t")
                    .append(" - DMS Applied")
                    .append("\n");
        });

        return sb.toString();
    }

    @Override
    public Set<MetricDatum> toCloudwatchMetricData() {
        Set<MetricDatum> metrics = new HashSet<>();

        addMissingTablesMetrics(metrics);
        addCountMetricsForDataStore("raw", rawZoneCounts, metrics);
        addCountMetricsForDataStore("dms", dmsCounts, metrics);
        addCountMetricsForDataStore("dmsApplied", dmsAppliedCounts, metrics);

        return metrics;
    }

    private void addMissingTablesMetrics(Set<MetricDatum> metricsToUpdate) {
        int inRawNotDms = tablesInRawMissingFromDms().size();

        if (inRawNotDms > 0) {
            metricsToUpdate.add(
                    new MetricDatum()
                            .withMetricName(IN_RAW_NOT_DMS_METRIC_NAME)
                            .withUnit(Count)
                            .withValue((double) inRawNotDms)
            );
        }

        int inDmsNotRaw = tablesInDmsMissingFromRaw().size();

        if (inDmsNotRaw > 0) {
            metricsToUpdate.add(
                    new MetricDatum()
                            .withMetricName(IN_DMS_NOT_RAW_METRIC_NAME)
                            .withUnit(Count)
                            .withValue((double) inDmsNotRaw)
            );
        }
    }


    private void addCountMetricsForDataStore(String dataStoreName, Map<String, ChangeDataTableCount> dataStoreCounts, Set<MetricDatum> metricsToUpdate) {
        for (Map.Entry<String, ChangeDataTableCount> entry : dataStoreCounts.entrySet()) {
            String tableName = entry.getKey();
            ChangeDataTableCount counts = entry.getValue();

            metricsToUpdate.add(changeDataCountMetricDatum(dataStoreName, Insert, tableName, counts.getInsertCount()));
            metricsToUpdate.add(changeDataCountMetricDatum(dataStoreName, Update, tableName, counts.getUpdateCount()));
            metricsToUpdate.add(changeDataCountMetricDatum(dataStoreName, Delete, tableName, counts.getDeleteCount()));

        }
    }

    private MetricDatum changeDataCountMetricDatum(String dataStore, CommonDataFields.ShortOperationCode op, String tableName, long count) {
        return new MetricDatum()
                .withMetricName(COUNT_METRIC_NAME)
                .withUnit(Count)
                .withDimensions(
                        // Data store, e.g. dms
                        new Dimension().withName("datastore").withValue(dataStore),
                        // Operation, e.g. U for update
                        new Dimension().withName("operation").withValue(op.getName()),
                        // The table name
                        new Dimension().withName("table").withValue(tableName)
                )
                .withValue((double) count);
    }

    private boolean rawCountsMatchDmsCounts() {
        return rawZoneCounts.entrySet().stream().allMatch(entry -> {
            String tableName = entry.getKey();
            ChangeDataTableCount rawCount = entry.getValue();
            ChangeDataTableCount dmsCount = dmsCounts.get(tableName);
            ChangeDataTableCount dmsAppliedCount = dmsAppliedCounts.get(tableName);
            return rawCount.equals(dmsCount) && dmsCount.equals(dmsAppliedCount);
        });
    }

    private boolean sameTables() {
        return rawZoneCounts.keySet().equals(dmsCounts.keySet()) && rawZoneCounts.keySet().equals(dmsAppliedCounts.keySet());
    }

    private Set<String> tablesInDmsMissingFromRaw() {
        HashSet<String> result = new HashSet<>(dmsCounts.keySet());
        result.removeAll(rawZoneCounts.keySet());
        return result;
    }

    private Set<String> tablesInRawMissingFromDms() {
        HashSet<String> result = new HashSet<>(rawZoneCounts.keySet());
        result.removeAll(dmsCounts.keySet());
        return result;
    }

    private String nullSafeCountString(ChangeDataTableCount count) {
        if (count != null) {
            return count.toString();
        } else {
            return MISSING_COUNTS_MESSAGE;
        }
    }
}
