package uk.gov.justice.digital.service.operationaldatastore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.datahub.model.SourceReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DisabledOperationalDataStoreServiceTest {

    @Mock
    private SourceReference sourceReference;

    private DisabledOperationalDataStoreService underTest;

    @BeforeEach
    void setUp() {
        underTest = new DisabledOperationalDataStoreService();
    }

    @Test
    void isEnabledShouldBeFalse() {
        assertFalse(underTest.isEnabled());
    }

    @Test
    void isOperationalDataStoreManagedTableShouldBeFalse() {
        when(sourceReference.getSource()).thenReturn("source");
        when(sourceReference.getTable()).thenReturn("table");
        assertFalse(underTest.isOperationalDataStoreManagedTable(sourceReference));
    }

    @Test
    void getTableRowCountShouldThrow() {
        assertThrows(IllegalStateException.class, () -> {
            underTest.getTableRowCount("table");
        });
    }

}