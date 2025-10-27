package uk.gov.justice.digital.service;

import com.google.common.collect.ImmutableSet;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.glue.GlueClient;
import uk.gov.justice.digital.config.SparkTestBase;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.HiveSchemaServiceException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static uk.gov.justice.digital.common.CommonDataFields.withMetadataFields;
import static uk.gov.justice.digital.common.CommonDataFields.withCheckpointField;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.test.Fixtures.JSON_DATA_SCHEMA;
import static uk.gov.justice.digital.test.Fixtures.TABLE_NAME;
import static uk.gov.justice.digital.test.TestHelpers.containsTheSameElementsInOrderAs;

@ExtendWith(MockitoExtension.class)
class HiveTableServiceTest extends SparkTestBase {

    @Mock
    private JobArguments mockJobArguments;
    @Mock
    private SourceReferenceService mockSourceReferenceService;
    @Mock
    private DataStorageService mockStorageService;
    @Mock
    private GlueClient mockGlueClient;

    @Captor
    ArgumentCaptor<String> deleteDatabaseArgCaptor, createArchiveDatabaseArgCaptor, createSymlinkDatabaseArgCaptor;

    @Captor
    ArgumentCaptor<String> deleteTableArgCaptor, createArchiveTableArgCaptor, createSymlinkTableArgCaptor;

    @Captor
    ArgumentCaptor<String> createArchivePathArgCaptor, createSymlinkPathArgCaptor, updateManifestTablePathCaptor;

    @Captor
    ArgumentCaptor<SourceReference.PrimaryKey> createArchivePrimaryKeyCaptor, createSymlinkPrimaryKeyCaptor;

    @Captor
    ArgumentCaptor<SourceReference.SensitiveColumns> createArchiveSensitiveColumnsCaptor;

    private HiveTableService underTest;

    private static final String RAW_ARCHIVE_BUCKET = "s3://raw-archive";
    private static final String STRUCTURED_ZONE_BUCKET = "s3://structured-zone";
    private static final String CURATED_ZONE_BUCKET = "s3://curated-zone";
    private static final String TEMP_RELOAD_BUCKET = "s3://temp-reload";
    private static final String RAW_ARCHIVE_DATABASE = "raw_archive";
    private static final String STRUCTURED_DATABASE = "structured";
    private static final String CURATED_DATABASE = "curated";
    private static final String PRISONS_DATABASE = "prisons";

    private static final String SCHEMA_NAME = "test_schema";
    private static final String TABLE = "test_table";

    @BeforeEach
    void setup() {
        reset(mockJobArguments, mockSourceReferenceService, mockStorageService, mockGlueClient);
        underTest = new HiveTableService(mockJobArguments, mockSourceReferenceService, mockStorageService, mockGlueClient);
    }

    @Test
    void replaceTablesShouldFailWhenThereAreNoSchemas() {
        ImmutableSet<ImmutablePair<String, String>> schemaGroup = ImmutableSet.of(ImmutablePair.of(SCHEMA_NAME, TABLE_NAME));

        when(mockSourceReferenceService.getAllSourceReferences(any())).thenReturn(Collections.emptyList());

        assertThrows(HiveSchemaServiceException.class, () -> underTest.replaceTables(schemaGroup));
    }

    @Test
    void replaceTablesShouldFailWhenSourceReferenceServiceThrowsAndException() {
        ImmutableSet<ImmutablePair<String, String>> schemaGroup = ImmutableSet.of(ImmutablePair.of(SCHEMA_NAME, TABLE_NAME));

        when(mockSourceReferenceService.getAllSourceReferences(any())).thenThrow(new RuntimeException("Source reference error"));

        assertThrows(RuntimeException.class, () -> underTest.replaceTables(schemaGroup));
    }

    @Test
    void replaceTablesShouldReplaceHiveTablesForSchemas() {
        Set<ImmutablePair<String, String>> schemaGroupSet = Stream.of(0, 1)
                .map(index -> ImmutablePair.of(createSchemaName(index), createTableName(index)))
                .collect(Collectors.toSet());

        ImmutableSet<ImmutablePair<String, String>> schemaGroup = ImmutableSet.copyOf(schemaGroupSet);

        List<SourceReference> sourceReferences = new ArrayList<>();
        sourceReferences.add(createSourceRef(0));
        sourceReferences.add(createSourceRef(1));

        val expectedCreateArchivePrimaryKeyArgs = createPrimaryKeys(Stream.of(0, 1));
        val expectedCreateArchiveSensitiveColumnsArgs = createSensitiveColumns(Stream.of(0, 1));
        val expectedSymlinkPrimaryKeyArgs = createPrimaryKeys(Stream.of(0, 0, 0, 1, 1, 1));

        List<String> expectedDeleteDatabaseArgs = new ArrayList<>();
        expectedDeleteDatabaseArgs.add(RAW_ARCHIVE_DATABASE);
        expectedDeleteDatabaseArgs.add(STRUCTURED_DATABASE);
        expectedDeleteDatabaseArgs.add(CURATED_DATABASE);
        expectedDeleteDatabaseArgs.add(PRISONS_DATABASE);
        expectedDeleteDatabaseArgs.add(RAW_ARCHIVE_DATABASE);
        expectedDeleteDatabaseArgs.add(STRUCTURED_DATABASE);
        expectedDeleteDatabaseArgs.add(CURATED_DATABASE);
        expectedDeleteDatabaseArgs.add(PRISONS_DATABASE);

        List<String> expectedDeleteTableArgs = createExpectedTableArgsFromSequence(Stream.of(0, 0, 0, 0, 1, 1, 1, 1));

        List<String> expectedCreateArchiveDatabaseArgs = new ArrayList<>();
        expectedCreateArchiveDatabaseArgs.add(RAW_ARCHIVE_DATABASE);
        expectedCreateArchiveDatabaseArgs.add(RAW_ARCHIVE_DATABASE);

        List<String> expectedCreateArchiveTableArgs = createExpectedTableArgsFromSequence(Stream.of(0, 1));

        List<String> expectedCreateArchivePathArgs = new ArrayList<>();
        expectedCreateArchivePathArgs.add(createPath(RAW_ARCHIVE_BUCKET, 0));
        expectedCreateArchivePathArgs.add(createPath(RAW_ARCHIVE_BUCKET, 1));

        List<String> expectedCreateSymlinkDatabaseArgs = new ArrayList<>();
        expectedCreateSymlinkDatabaseArgs.add(STRUCTURED_DATABASE);
        expectedCreateSymlinkDatabaseArgs.add(CURATED_DATABASE);
        expectedCreateSymlinkDatabaseArgs.add(PRISONS_DATABASE);
        expectedCreateSymlinkDatabaseArgs.add(STRUCTURED_DATABASE);
        expectedCreateSymlinkDatabaseArgs.add(CURATED_DATABASE);
        expectedCreateSymlinkDatabaseArgs.add(PRISONS_DATABASE);

        List<String> expectedCreateSymlinkTableArgs = createExpectedTableArgsFromSequence(Stream.of(0, 0, 0, 1, 1, 1));

        List<String> expectedCreateSymlinkPathArgs = new ArrayList<>();
        expectedCreateSymlinkPathArgs.add(createPath(STRUCTURED_ZONE_BUCKET, 0));
        expectedCreateSymlinkPathArgs.add(createPath(CURATED_ZONE_BUCKET, 0));
        expectedCreateSymlinkPathArgs.add(createPath(CURATED_ZONE_BUCKET, 0));
        expectedCreateSymlinkPathArgs.add(createPath(STRUCTURED_ZONE_BUCKET, 1));
        expectedCreateSymlinkPathArgs.add(createPath(CURATED_ZONE_BUCKET, 1));
        expectedCreateSymlinkPathArgs.add(createPath(CURATED_ZONE_BUCKET, 1));

        mockJobArgumentCalls();
        when(mockSourceReferenceService.getAllSourceReferences(any())).thenReturn(sourceReferences);

        assertThat((Collection<ImmutablePair<String, String>>) underTest.replaceTables(schemaGroup), is(empty()));

        // verify all Hive tables get deleted
        verify(mockGlueClient, times(8))
                .deleteTable(deleteDatabaseArgCaptor.capture(), deleteTableArgCaptor.capture());

        assertThat(deleteDatabaseArgCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedDeleteDatabaseArgs));
        assertThat(deleteTableArgCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedDeleteTableArgs));

        // verify raw_archive tables are created as parquet format
        verify(mockGlueClient, times(2))
                .createParquetTable(
                        createArchiveDatabaseArgCaptor.capture(),
                        createArchiveTableArgCaptor.capture(),
                        createArchivePathArgCaptor.capture(),
                        eq(withCheckpointField(withMetadataFields(JSON_DATA_SCHEMA))),
                        createArchivePrimaryKeyCaptor.capture(),
                        createArchiveSensitiveColumnsCaptor.capture()
                );

        assertOnCreateParquetTableArgs(
                expectedCreateArchiveDatabaseArgs,
                expectedCreateArchiveTableArgs,
                expectedCreateArchivePathArgs,
                expectedCreateArchivePrimaryKeyArgs,
                expectedCreateArchiveSensitiveColumnsArgs,
                createArchiveDatabaseArgCaptor,
                createArchiveTableArgCaptor,
                createArchivePathArgCaptor,
                createArchivePrimaryKeyCaptor,
                createArchiveSensitiveColumnsCaptor
        );

        // verify structured, curated and prisons tables are created with symlink format
        verify(mockGlueClient, times(6))
                .createTableWithSymlink(
                        createSymlinkDatabaseArgCaptor.capture(),
                        createSymlinkTableArgCaptor.capture(),
                        createSymlinkPathArgCaptor.capture(),
                        eq(JSON_DATA_SCHEMA),
                        createSymlinkPrimaryKeyCaptor.capture()
                );

        assertOnCreateSymlinkedTableArgs(
                expectedCreateSymlinkDatabaseArgs,
                expectedCreateSymlinkTableArgs,
                expectedCreateSymlinkPathArgs,
                expectedSymlinkPrimaryKeyArgs,
                createSymlinkDatabaseArgCaptor,
                createSymlinkTableArgCaptor,
                createSymlinkPathArgCaptor,
                createSymlinkPrimaryKeyCaptor
        );
    }

    @Test
    void switchPrisonsTableDataSourceShouldFailWhenThereAreNoSchemas() {
        ImmutableSet<ImmutablePair<String, String>> schemaGroup = ImmutableSet.of(ImmutablePair.of(SCHEMA_NAME, TABLE_NAME));

        when(mockSourceReferenceService.getAllSourceReferences(any())).thenReturn(Collections.emptyList());

        assertThrows(HiveSchemaServiceException.class, () -> underTest.switchPrisonsTableDataSource(spark, schemaGroup));
    }

    @Test
    void switchPrisonsTableDataSourceShouldFailWhenSourceReferenceServiceThrowsAndException() {
        ImmutableSet<ImmutablePair<String, String>> schemaGroup = ImmutableSet.of(ImmutablePair.of(SCHEMA_NAME, TABLE_NAME));

        when(mockSourceReferenceService.getAllSourceReferences(any())).thenThrow(new RuntimeException("Source reference error"));

        assertThrows(RuntimeException.class, () -> underTest.switchPrisonsTableDataSource(spark, schemaGroup));
    }

    @Test
    void switchPrisonsTableDataSourceShouldLinkPrisonsTableToSpecifiedSource() {
        Set<ImmutablePair<String, String>> schemaGroupSet = Stream.of(0, 1)
                .map(index -> ImmutablePair.of(createSchemaName(index), createTableName(index)))
                .collect(Collectors.toSet());

        ImmutableSet<ImmutablePair<String, String>> schemaGroup = ImmutableSet.copyOf(schemaGroupSet);

        List<SourceReference> sourceReferences = new ArrayList<>();
        sourceReferences.add(createSourceRef(0));
        sourceReferences.add(createSourceRef(1));

        List<SourceReference.PrimaryKey> expectedPrimaryKeyArgs = createPrimaryKeys(Stream.of(0, 1));

        List<String> expectedDeleteDatabaseArgs = new ArrayList<>();
        expectedDeleteDatabaseArgs.add(PRISONS_DATABASE);
        expectedDeleteDatabaseArgs.add(PRISONS_DATABASE);

        List<String> expectedDeleteTableArgs = createExpectedTableArgsFromSequence(Stream.of(0, 1));

        List<String> expectedCreateSymlinkDatabaseArgs = new ArrayList<>();
        expectedCreateSymlinkDatabaseArgs.add(PRISONS_DATABASE);
        expectedCreateSymlinkDatabaseArgs.add(PRISONS_DATABASE);

        List<String> expectedCreateSymlinkTableArgs = createExpectedTableArgsFromSequence(Stream.of(0, 1));

        List<String> expectedCreateSymlinkPathArgs = new ArrayList<>();
        String tablePath0 = createPath(TEMP_RELOAD_BUCKET, 0);
        expectedCreateSymlinkPathArgs.add(tablePath0);
        String tablePath1 = createPath(TEMP_RELOAD_BUCKET, 1);
        expectedCreateSymlinkPathArgs.add(tablePath1);

        when(mockJobArguments.getPrisonsDataSwitchTargetS3Path()).thenReturn(TEMP_RELOAD_BUCKET);
        when(mockJobArguments.getPrisonsDatabase()).thenReturn(PRISONS_DATABASE);
        when(mockSourceReferenceService.getAllSourceReferences(any())).thenReturn(sourceReferences);
        doNothing().when(mockStorageService).updateDeltaManifestForTable(eq(spark), updateManifestTablePathCaptor.capture());

        assertThat((Collection<ImmutablePair<String, String>>) underTest.switchPrisonsTableDataSource(spark, schemaGroup), is(empty()));

        // verify configured Hive tables get deleted
        verify(mockGlueClient, times(2))
                .deleteTable(deleteDatabaseArgCaptor.capture(), deleteTableArgCaptor.capture());

        assertThat(deleteDatabaseArgCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedDeleteDatabaseArgs));
        assertThat(deleteTableArgCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedDeleteTableArgs));

        // verify prisons tables are created with symlink format
        verify(mockGlueClient, times(2))
                .createTableWithSymlink(
                        createSymlinkDatabaseArgCaptor.capture(),
                        createSymlinkTableArgCaptor.capture(),
                        createSymlinkPathArgCaptor.capture(),
                        eq(JSON_DATA_SCHEMA),
                        createSymlinkPrimaryKeyCaptor.capture()
                );

        assertOnCreateSymlinkedTableArgs(
                expectedCreateSymlinkDatabaseArgs,
                expectedCreateSymlinkTableArgs,
                expectedCreateSymlinkPathArgs,
                expectedPrimaryKeyArgs,
                createSymlinkDatabaseArgCaptor,
                createSymlinkTableArgCaptor,
                createSymlinkPathArgCaptor,
                createSymlinkPrimaryKeyCaptor
        );

        assertThat(updateManifestTablePathCaptor.getAllValues(), containsInAnyOrder(tablePath0, tablePath1));
    }

    private void assertOnCreateParquetTableArgs(
            List<String> expectedDatabaseArgs,
            List<String> expectedTableArgs,
            List<String> expectedPathArgs,
            List<SourceReference.PrimaryKey> expectedPrimaryKeyArgs,
            List<SourceReference.SensitiveColumns> expectedSensitiveColumnsArgs,
            ArgumentCaptor<String> databaseArgCaptor,
            ArgumentCaptor<String> tableArgCaptor,
            ArgumentCaptor<String> pathCaptor,
            ArgumentCaptor<SourceReference.PrimaryKey> primaryKeyArgCaptor,
            ArgumentCaptor<SourceReference.SensitiveColumns> sensitiveColumnsArgCaptor
    ) {
        assertThat(databaseArgCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedDatabaseArgs));
        assertThat(tableArgCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedTableArgs));
        assertThat(pathCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedPathArgs));
        assertPrimaryKeysMatch(primaryKeyArgCaptor.getAllValues(), expectedPrimaryKeyArgs);
        assertSensitiveColumnsMatch(sensitiveColumnsArgCaptor.getAllValues(), expectedSensitiveColumnsArgs);
    }

    private void assertOnCreateSymlinkedTableArgs(
            List<String> expectedDatabaseArgs,
            List<String> expectedTableArgs,
            List<String> expectedPathArgs,
            List<SourceReference.PrimaryKey> expectedPrimaryKeyArgs,
            ArgumentCaptor<String> databaseArgCaptor,
            ArgumentCaptor<String> tableArgCaptor,
            ArgumentCaptor<String> pathCaptor,
            ArgumentCaptor<SourceReference.PrimaryKey> primaryKeyArgCaptor
    ) {
        assertThat(databaseArgCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedDatabaseArgs));
        assertThat(tableArgCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedTableArgs));
        assertThat(pathCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedPathArgs));
        assertPrimaryKeysMatch(primaryKeyArgCaptor.getAllValues(), expectedPrimaryKeyArgs);
    }

    @NotNull
    private static List<String> createExpectedTableArgsFromSequence(Stream<Integer> schemaIndexes) {
        List<String> argList = new ArrayList<>();
        schemaIndexes.forEach(schemaIndex -> argList.add(createHiveTableName(schemaIndex)));
        return argList;
    }

    @NotNull
    private static List<SourceReference.PrimaryKey> createPrimaryKeys(Stream<Integer> indices) {
        return indices.map(index -> new SourceReference.PrimaryKey(String.valueOf(index))).collect(Collectors.toList());
    }

    @NotNull
    private static List<SourceReference.SensitiveColumns> createSensitiveColumns(Stream<Integer> indices) {
        return indices.map(HiveTableServiceTest::createSensitiveColumn).collect(Collectors.toList());
    }

    private static String createPath(String bucket, int schemaIndex) {
        return createValidatedPath(bucket, createSchemaName(schemaIndex), createTableName(schemaIndex));
    }

    @NotNull
    private static String createHiveTableName(int index) {
        return String.format("%s_%s", createSchemaName(index), createTableName(index));
    }

    @NotNull
    private static String createSchemaName(int index) {
        return String.format("%s%d", SCHEMA_NAME, index);
    }

    @NotNull
    private static String createTableName(int index) {
        return String.format("%s%d", TABLE, index);
    }

    @NotNull
    private static SourceReference createSourceRef(int index) {
        String key = String.valueOf(index);
        val primaryKey = new SourceReference.PrimaryKey(key);
        val sensitiveColumn = createSensitiveColumn(index);
        String namespace = "prisons";
        String source = SCHEMA_NAME + key;
        String table = TABLE + key;
        String versionId = UUID.randomUUID().toString();
        return new SourceReference(key, namespace, source, table, primaryKey, versionId, JSON_DATA_SCHEMA, sensitiveColumn);
    }

    @NotNull
    private static SourceReference.SensitiveColumns createSensitiveColumn(Integer index) {
        return new SourceReference.SensitiveColumns("sensitive_" + index);
    }

    private void mockJobArgumentCalls() {
        when(mockJobArguments.getRawArchiveS3Path()).thenReturn(RAW_ARCHIVE_BUCKET);
        when(mockJobArguments.getStructuredS3Path()).thenReturn(STRUCTURED_ZONE_BUCKET);
        when(mockJobArguments.getCuratedS3Path()).thenReturn(CURATED_ZONE_BUCKET);

        when(mockJobArguments.getRawArchiveDatabase()).thenReturn(RAW_ARCHIVE_DATABASE);
        when(mockJobArguments.getStructuredDatabase()).thenReturn(STRUCTURED_DATABASE);
        when(mockJobArguments.getCuratedDatabase()).thenReturn(CURATED_DATABASE);
        when(mockJobArguments.getPrisonsDatabase()).thenReturn(PRISONS_DATABASE);
    }

    private static void assertPrimaryKeysMatch(List<SourceReference.PrimaryKey> actual, List<SourceReference.PrimaryKey> expected) {
        assertThat(
                getPrimaryKeyColumnNames(actual),
                containsTheSameElementsInOrderAs(getPrimaryKeyColumnNames(expected))
        );
    }

    private static void assertSensitiveColumnsMatch(List<SourceReference.SensitiveColumns> actual, List<SourceReference.SensitiveColumns> expected) {
        assertThat(
                getSensitiveColumnNames(actual),
                containsTheSameElementsInOrderAs(getSensitiveColumnNames(expected))
        );
    }

    @NotNull
    private static List<String> getPrimaryKeyColumnNames(List<SourceReference.PrimaryKey> values) {
        return values.stream().flatMap(primaryKey -> primaryKey.getKeyColumnNames().stream()).collect(Collectors.toList());
    }

    @NotNull
    private static List<String> getSensitiveColumnNames(List<SourceReference.SensitiveColumns> values) {
        return values.stream()
                .flatMap(sensitiveColumns -> sensitiveColumns.getSensitiveColumnNames().stream())
                .collect(Collectors.toList());
    }

}
