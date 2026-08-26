package uk.gov.justice.digital.service;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import uk.gov.justice.digital.common.ResourcePath;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.RawArchiveVersioningException;

import java.util.Optional;

import static uk.gov.justice.digital.common.ResourcePath.ensureEndsWithSlash;

/*
 * Single place that knows whether raw archive paths are versioned (schema/table/<version>/...) and what the
 * current version is. Readers and writers should go through here rather than checking the flag themselves -
 * once we resolve the version from DynamoDB instead of a config value, this is the only class that changes.
 * Version is resolved once per job run, not per table.
 */
@Singleton
public class RawArchiveLocationService {

    private final boolean versioningEnabled;
    private final Optional<String> currentVersion;

    @Inject
    public RawArchiveLocationService(JobArguments jobArguments) {
        this.versioningEnabled = jobArguments.isRawArchiveVersionedPathsEnabled();
        this.currentVersion = versioningEnabled ? Optional.of(resolveVersion(jobArguments)) : Optional.empty();
    }

    public boolean isVersioningEnabled() {
        return versioningEnabled;
    }

    public Optional<String> currentVersion() {
        return currentVersion;
    }

    // root/source/table, with /<version> appended when versioning is on.
    public String tablePath(String root, String source, String table) {
        String path = ResourcePath.tablePath(root, source, table);
        return currentVersion
                .map(version -> ensureEndsWithSlash(path) + version)
                .orElse(path);
    }

    /*
     * Inserts the version after the table segment of a raw object key, e.g. schema/table/file.parquet ->
     * schema/table/<version>/file.parquet. No-op when versioning is off.
     */
    public String applyVersionToKey(String objectKey) {
        if (!currentVersion.isPresent()) return objectKey;

        int firstSlash = objectKey.indexOf('/');
        int secondSlash = firstSlash < 0 ? -1 : objectKey.indexOf('/', firstSlash + 1);
        if (secondSlash < 0) {
            throw new RawArchiveVersioningException(
                    "Cannot apply raw archive version to key with unexpected format (expected schema/table/...): " + objectKey
            );
        }

        String schemaAndTable = objectKey.substring(0, secondSlash);
        String remainder = objectKey.substring(secondSlash);
        return schemaAndTable + "/" + currentVersion.get() + remainder;
    }

    private static String resolveVersion(JobArguments jobArguments) {
        return jobArguments.getRawArchiveVersion()
                .filter(version -> !version.trim().isEmpty())
                .orElseThrow(() -> new RawArchiveVersioningException(
                        JobArguments.RAW_ARCHIVE_VERSION + " must be set when " +
                                JobArguments.RAW_ARCHIVE_VERSIONED_PATHS_ENABLED + " is true"
                ));
    }
}
