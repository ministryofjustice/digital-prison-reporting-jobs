package uk.gov.justice.digital.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.RawArchiveVersioningException;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RawArchiveLocationServiceTest {

    private static final String VERSION = "v2";

    @Mock
    private JobArguments jobArguments;

    @Test
    void shouldReportVersioningDisabledByDefault() {
        when(jobArguments.isRawArchiveVersionedPathsEnabled()).thenReturn(false);

        RawArchiveLocationService underTest = new RawArchiveLocationService(jobArguments);

        assertFalse(underTest.isVersioningEnabled());
        assertEquals(Optional.empty(), underTest.currentVersion());
    }

    @Test
    void shouldResolveConfiguredVersionWhenVersioningEnabled() {
        when(jobArguments.isRawArchiveVersionedPathsEnabled()).thenReturn(true);
        when(jobArguments.getRawArchiveVersion()).thenReturn(Optional.of(VERSION));

        RawArchiveLocationService underTest = new RawArchiveLocationService(jobArguments);

        assertTrue(underTest.isVersioningEnabled());
        assertEquals(Optional.of(VERSION), underTest.currentVersion());
    }

    @Test
    void shouldFailFastWhenVersioningEnabledButNoVersionConfigured() {
        when(jobArguments.isRawArchiveVersionedPathsEnabled()).thenReturn(true);
        when(jobArguments.getRawArchiveVersion()).thenReturn(Optional.empty());

        assertThrows(RawArchiveVersioningException.class, () -> new RawArchiveLocationService(jobArguments));
    }

    @Test
    void shouldFailFastWhenVersioningEnabledButVersionIsBlank() {
        when(jobArguments.isRawArchiveVersionedPathsEnabled()).thenReturn(true);
        when(jobArguments.getRawArchiveVersion()).thenReturn(Optional.of("   "));

        assertThrows(RawArchiveVersioningException.class, () -> new RawArchiveLocationService(jobArguments));
    }

    @Test
    void tablePathShouldNotAppendVersionWhenVersioningDisabled() {
        when(jobArguments.isRawArchiveVersionedPathsEnabled()).thenReturn(false);
        RawArchiveLocationService underTest = new RawArchiveLocationService(jobArguments);

        assertEquals("s3://root/source/table", underTest.tablePath("s3://root", "source", "table"));
    }

    @Test
    void tablePathShouldAppendVersionWhenVersioningEnabled() {
        when(jobArguments.isRawArchiveVersionedPathsEnabled()).thenReturn(true);
        when(jobArguments.getRawArchiveVersion()).thenReturn(Optional.of(VERSION));
        RawArchiveLocationService underTest = new RawArchiveLocationService(jobArguments);

        assertEquals("s3://root/source/table/" + VERSION, underTest.tablePath("s3://root", "source", "table"));
    }

    @Test
    void applyVersionToKeyShouldReturnKeyUnchangedWhenVersioningDisabled() {
        when(jobArguments.isRawArchiveVersionedPathsEnabled()).thenReturn(false);
        RawArchiveLocationService underTest = new RawArchiveLocationService(jobArguments);

        assertEquals("schema/table/file.parquet", underTest.applyVersionToKey("schema/table/file.parquet"));
    }

    @Test
    void applyVersionToKeyShouldInsertVersionAfterTableSegmentWhenVersioningEnabled() {
        when(jobArguments.isRawArchiveVersionedPathsEnabled()).thenReturn(true);
        when(jobArguments.getRawArchiveVersion()).thenReturn(Optional.of(VERSION));
        RawArchiveLocationService underTest = new RawArchiveLocationService(jobArguments);

        assertEquals(
                "schema/table/" + VERSION + "/file.parquet",
                underTest.applyVersionToKey("schema/table/file.parquet")
        );
    }

    @Test
    void applyVersionToKeyShouldFailWhenKeyDoesNotHaveASchemaAndTableSegment() {
        when(jobArguments.isRawArchiveVersionedPathsEnabled()).thenReturn(true);
        when(jobArguments.getRawArchiveVersion()).thenReturn(Optional.of(VERSION));
        RawArchiveLocationService underTest = new RawArchiveLocationService(jobArguments);

        assertThrows(RawArchiveVersioningException.class, () -> underTest.applyVersionToKey("file.parquet"));
    }
}
