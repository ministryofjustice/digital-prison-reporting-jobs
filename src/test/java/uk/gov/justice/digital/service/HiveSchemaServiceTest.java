package uk.gov.justice.digital.service;

import lombok.val;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.glue.GlueHiveTableClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.HiveSchemaServiceException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.service.HiveSchemaService.*;
import static uk.gov.justice.digital.test.Fixtures.JSON_DATA_SCHEMA;
import static uk.gov.justice.digital.test.SparkTestHelpers.containsTheSameElementsInOrderAs;

@ExtendWith(MockitoExtension.class)
public class HiveSchemaServiceTest {

    @Mock
    private JobArguments mockJobArguments;
    @Mock
    private SourceReferenceService mockSourceReferenceService;
    @Mock
    private GlueHiveTableClient mockGlueHiveTableClient;

    @Captor
    ArgumentCaptor<String> deleteDatabaseArgCaptor, createParquetDatabaseArgCaptor, createSymlinkDatabaseArgCaptor;

    @Captor
    ArgumentCaptor<String> deleteTableArgCaptor, createParquetTableArgCaptor, createSymlinkTableArgCaptor;

    @Captor
    ArgumentCaptor<String> createParquetPathArgCaptor, createSymlinkPathArgCaptor;

    private HiveSchemaService underTest;

    private static final String STRUCTURED_ZONE_BUCKET = "s3://structured-zone";
    private static final String CURATED_ZONE_BUCKET = "s3://curated-zone";

    private static final String SCHEMA_NAME = "test_schema";
    private static final String TABLE = "test_table";

    @BeforeEach
    public void setup() {
        underTest = new HiveSchemaService(mockJobArguments, mockSourceReferenceService, mockGlueHiveTableClient);
    }

    @Test
    public void shouldFailWhenThereAreNoSchemas() {
        when(mockSourceReferenceService.getAllSourceReferences()).thenReturn(Collections.emptyList());

        assertThrows(HiveSchemaServiceException.class, () -> underTest.replaceTables());
    }

    @Test
    public void shouldFailWhenSourceReferenceServiceThrowsAndException() {
        when(mockSourceReferenceService.getAllSourceReferences()).thenThrow(new RuntimeException("Source reference error"));

        assertThrows(RuntimeException.class, () -> underTest.replaceTables());
    }

    @Test
    public void shouldReplaceHiveTablesForSchemas() {
        List<SourceReference> sourceReferences = new ArrayList<>();
        sourceReferences.add(createSourceRef(0));
        sourceReferences.add(createSourceRef(1));

        List<String> expectedDeleteDatabaseArgs = new ArrayList<>();
        expectedDeleteDatabaseArgs.add(STRUCTURED_DATABASE);
        expectedDeleteDatabaseArgs.add(CURATED_DATABASE);
        expectedDeleteDatabaseArgs.add(PRISONS_DATABASE);
        expectedDeleteDatabaseArgs.add(STRUCTURED_DATABASE);
        expectedDeleteDatabaseArgs.add(CURATED_DATABASE);
        expectedDeleteDatabaseArgs.add(PRISONS_DATABASE);

        List<String> expectedDeleteTableArgs = createExpectedTableArgs(Stream.of(0, 0, 0, 1, 1, 1));

        List<String> expectedCreateParquetDatabaseArgs = new ArrayList<>();
        expectedCreateParquetDatabaseArgs.add(STRUCTURED_DATABASE);
        expectedCreateParquetDatabaseArgs.add(CURATED_DATABASE);
        expectedCreateParquetDatabaseArgs.add(STRUCTURED_DATABASE);
        expectedCreateParquetDatabaseArgs.add(CURATED_DATABASE);

        List<String> expectedCreateParquetTableArgs = createExpectedTableArgs(Stream.of(0, 0, 1, 1));

        List<String> expectedCreateParquetPathArgs = new ArrayList<>();
        expectedCreateParquetPathArgs.add(createPath(STRUCTURED_ZONE_BUCKET, 0));
        expectedCreateParquetPathArgs.add(createPath(CURATED_ZONE_BUCKET, 0));
        expectedCreateParquetPathArgs.add(createPath(STRUCTURED_ZONE_BUCKET, 1));
        expectedCreateParquetPathArgs.add(createPath(CURATED_ZONE_BUCKET, 1));

        List<String> expectedCreateSymlinkDatabaseArgs = new ArrayList<>();
        expectedCreateSymlinkDatabaseArgs.add(PRISONS_DATABASE);
        expectedCreateSymlinkDatabaseArgs.add(PRISONS_DATABASE);

        List<String> expectedCreateSymlinkTableArgs = createExpectedTableArgs(Stream.of(0, 1));

        List<String> expectedCreateSymlinkPathArgs = new ArrayList<>();
        expectedCreateSymlinkPathArgs.add(createPath(CURATED_ZONE_BUCKET, 0));
        expectedCreateSymlinkPathArgs.add(createPath(CURATED_ZONE_BUCKET, 1));

        mockJobArgumentCalls();
        when(mockSourceReferenceService.getAllSourceReferences()).thenReturn(sourceReferences);

        assertThat((Collection<String>) underTest.replaceTables(), is(empty()));

        verify(mockGlueHiveTableClient, times(6))
                .deleteTable(deleteDatabaseArgCaptor.capture(), deleteTableArgCaptor.capture());

        verify(mockGlueHiveTableClient, times(4))
                .createParquetTable(
                        createParquetDatabaseArgCaptor.capture(),
                        createParquetTableArgCaptor.capture(),
                        createParquetPathArgCaptor.capture(),
                        eq(JSON_DATA_SCHEMA)
                );

        verify(mockGlueHiveTableClient, times(2))
                .createTableWithSymlink(
                        createSymlinkDatabaseArgCaptor.capture(),
                        createSymlinkTableArgCaptor.capture(),
                        createSymlinkPathArgCaptor.capture(),
                        eq(JSON_DATA_SCHEMA)
                );

        assertThat(deleteDatabaseArgCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedDeleteDatabaseArgs));
        assertThat(deleteTableArgCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedDeleteTableArgs));

        assertThat(createParquetDatabaseArgCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedCreateParquetDatabaseArgs));
        assertThat(createParquetTableArgCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedCreateParquetTableArgs));
        assertThat(createParquetPathArgCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedCreateParquetPathArgs));

        assertThat(createSymlinkDatabaseArgCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedCreateSymlinkDatabaseArgs));
        assertThat(createSymlinkTableArgCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedCreateSymlinkTableArgs));
        assertThat(createSymlinkPathArgCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedCreateSymlinkPathArgs));
    }

    @NotNull
    private static List<String> createExpectedTableArgs(Stream<Integer> schemaIndexes) {
        List<String> argList = new ArrayList<>();
        schemaIndexes.forEach(schemaIndex -> argList.add(createHiveTableName(schemaIndex)));
        return argList;
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
        String source = SCHEMA_NAME + key;
        String table = TABLE + key;
        return new SourceReference(key, source, table, primaryKey, JSON_DATA_SCHEMA);
    }

    private void mockJobArgumentCalls() {
        when(mockJobArguments.getStructuredS3Path()).thenReturn(STRUCTURED_ZONE_BUCKET);
        when(mockJobArguments.getCuratedS3Path()).thenReturn(CURATED_ZONE_BUCKET);
    }

}
