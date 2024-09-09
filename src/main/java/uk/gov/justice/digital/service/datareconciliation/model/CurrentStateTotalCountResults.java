package uk.gov.justice.digital.service.datareconciliation.model;

import lombok.val;
import uk.gov.justice.digital.datahub.model.SourceReference;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the results of running the total counts data reconciliation for the "current state" data in DataHub
 */
public class CurrentStateTotalCountResults {

    private final Map<SourceReference, CurrentStateCountTableResult> results = new HashMap<>();

    public void put(SourceReference sourceReference, CurrentStateCountTableResult currentStateCountTableResult) {
        results.put(sourceReference, currentStateCountTableResult);
    }

    public boolean isFailure() {
        return !results.entrySet().stream().allMatch(entry -> entry.getValue().countsMatch());
    }

    public String summary() {
        StringBuilder sb = new StringBuilder("Current State Count Results:\n");

        for (val entrySet: results.entrySet()) {
            val sourceReference = entrySet.getKey();
            val currentStateCountTableResult = entrySet.getValue();
            sb.append("For table ");
            sb.append(sourceReference.getSource()).append(".").append(sourceReference.getTable());
            sb.append(":\n");
            sb.append(currentStateCountTableResult.summary());
            sb.append("\n");
        }

        return sb.toString();
    }
}
