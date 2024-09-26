package uk.gov.justice.digital.service.datareconciliation.model;

/**
 * The data reconciliation checks that can be run by the Data Reconciliation service
 */
public enum ReconciliationCheck {
    CURRENT_STATE_ROW_DIFFERENCES,
    CURRENT_STATE_COUNTS,
    CHANGE_DATA_COUNTS;

    public static ReconciliationCheck fromString(String reconciliationType) {
        switch (reconciliationType.trim().toLowerCase()) {
            case "current_state_counts":
                return CURRENT_STATE_COUNTS;
            case "change_data_counts":
                return CHANGE_DATA_COUNTS;
            case "current_state_row_differences":
                return CURRENT_STATE_ROW_DIFFERENCES;
            default:
                throw new IllegalArgumentException("Unknown reconciliation type: " + reconciliationType);
        }
    }
}
