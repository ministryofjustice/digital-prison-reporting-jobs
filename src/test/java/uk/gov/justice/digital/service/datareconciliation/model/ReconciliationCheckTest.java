package uk.gov.justice.digital.service.datareconciliation.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static uk.gov.justice.digital.service.datareconciliation.model.ReconciliationCheck.CHANGE_DATA_COUNTS;
import static uk.gov.justice.digital.service.datareconciliation.model.ReconciliationCheck.CURRENT_STATE_COUNTS;
import static uk.gov.justice.digital.service.datareconciliation.model.ReconciliationCheck.PRIMARY_KEY_RECONCILIATION;

class ReconciliationCheckTest {

    @Test
    void shouldMapCurrentStateCounts() {
        assertEquals(CURRENT_STATE_COUNTS, ReconciliationCheck.fromString("current_state_counts"));
    }

    @Test
    void shouldMapChangeDataCounts() {
        assertEquals(CHANGE_DATA_COUNTS, ReconciliationCheck.fromString("change_data_counts"));
    }

    @Test
    void shouldMapPrimaryKeyReconciliation() {
        assertEquals(PRIMARY_KEY_RECONCILIATION, ReconciliationCheck.fromString("primary_key_reconciliation"));
    }

    @Test
    void shouldThrowForUnrecognisedReconciliationCheck() {
        assertThrows(IllegalArgumentException.class, () -> ReconciliationCheck.fromString("unknown"));
    }
}