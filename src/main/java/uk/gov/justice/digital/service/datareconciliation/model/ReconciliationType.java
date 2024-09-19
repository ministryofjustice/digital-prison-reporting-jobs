package uk.gov.justice.digital.service.datareconciliation.model;

public enum ReconciliationType {
    CURRENT_STATE_COUNTS,
    CHANGE_DATA_COUNTS;

    public static ReconciliationType fromString(String reconciliationType) {
        switch (reconciliationType.trim().toLowerCase()) {
            case "current_state_counts":
                return CURRENT_STATE_COUNTS;
            case "change_data_counts":
                return CHANGE_DATA_COUNTS;
            default:
                throw new IllegalArgumentException("Unknown reconciliation type: " + reconciliationType);
        }
    }
}
