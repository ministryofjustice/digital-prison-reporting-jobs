package uk.gov.justice.digital.client.dms;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.databasemigrationservice.AWSDatabaseMigrationService;
import com.amazonaws.services.databasemigrationservice.model.DescribeReplicationTasksRequest;
import com.amazonaws.services.databasemigrationservice.model.DescribeReplicationTasksResult;
import com.amazonaws.services.databasemigrationservice.model.DescribeTableStatisticsRequest;
import com.amazonaws.services.databasemigrationservice.model.DescribeTableStatisticsResult;
import com.amazonaws.services.databasemigrationservice.model.Filter;
import com.amazonaws.services.databasemigrationservice.model.ModifyReplicationTaskRequest;
import com.amazonaws.services.databasemigrationservice.model.ModifyReplicationTaskResult;
import com.amazonaws.services.databasemigrationservice.model.ReplicationTask;
import com.amazonaws.services.databasemigrationservice.model.StopReplicationTaskRequest;
import com.amazonaws.services.databasemigrationservice.model.StopReplicationTaskResult;
import com.amazonaws.services.databasemigrationservice.model.TableStatistics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.exception.DmsClientException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DmsClientTest {

    @Mock
    private DmsClientProvider mockClientProvider;
    @Mock
    private AWSDatabaseMigrationService mockDmsClient;
    @Mock
    private DescribeReplicationTasksResult mockDescribeReplicationTasksResult;
    @Mock
    private StopReplicationTaskResult mockStopReplicationTaskResult;
    @Mock
    private DescribeTableStatisticsResult mockDescribeTableStatisticsResult;
    @Captor
    private ArgumentCaptor<DescribeReplicationTasksRequest> describeReplicationTasksRequestCaptor;
    @Captor
    private ArgumentCaptor<StopReplicationTaskRequest> stopReplicationTaskRequestCaptor;
    @Captor
    private ArgumentCaptor<DescribeTableStatisticsRequest> describeTableStatisticsRequestCaptor;
    @Captor
    private ArgumentCaptor<ModifyReplicationTaskRequest> modifyReplicationTaskRequestCaptor;

    private static final String TEST_TASK_ID = "test_task_id";
    private static final String TASK_TASK_ARN = "test_replication_task_arn";
    private static final String DOMAIN_KEY = "test_domain";
    private static final int WAIT_INTERVAL_SECONDS = 1;
    private static final int MAX_ATTEMPTS = 1;

    private DmsClient underTest;

    @BeforeEach
    void setup() {
        reset(mockClientProvider, mockDmsClient, mockDescribeReplicationTasksResult, mockStopReplicationTaskResult, mockDescribeTableStatisticsResult);

        when(mockClientProvider.getClient()).thenReturn(mockDmsClient);
        underTest = new DmsClient(mockClientProvider);
    }

    @SuppressWarnings("unchecked")
    @Test
    void stopTaskShouldStopRunningDmsTaskWhenThereIsOne() {
        List<ReplicationTask> runningTasks = new ArrayList<>();
        runningTasks.add(createReplicationTask("running").withReplicationTaskArn("replication-task-arn"));

        List<ReplicationTask> stoppedTasks = new ArrayList<>();
        stoppedTasks.add(createReplicationTask("stopped").withReplicationTaskArn("replication-task-arn"));

        when(mockDescribeReplicationTasksResult.getReplicationTasks()).thenReturn(runningTasks, stoppedTasks);
        when(mockDmsClient.describeReplicationTasks(describeReplicationTasksRequestCaptor.capture()))
                .thenReturn(mockDescribeReplicationTasksResult);
        when(mockDmsClient.stopReplicationTask(stopReplicationTaskRequestCaptor.capture()))
                .thenReturn(mockStopReplicationTaskResult);

        underTest.stopTask(TEST_TASK_ID, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS);

        StopReplicationTaskRequest stopReplicationTaskRequest = stopReplicationTaskRequestCaptor.getValue();
        assertThat(stopReplicationTaskRequest.getReplicationTaskArn(), equalTo("replication-task-arn"));
        verifyDescribeReplicationTasksRequestParams(describeReplicationTasksRequestCaptor.getValue());
    }

    @Test
    void stopTaskShouldNotFailWhenTaskIsAlreadyStopped() {
        List<ReplicationTask> stoppedTasks = new ArrayList<>();
        stoppedTasks.add(createReplicationTask("stopped").withReplicationTaskArn("replication-task-arn"));

        when(mockDescribeReplicationTasksResult.getReplicationTasks()).thenReturn(stoppedTasks);
        when(mockDmsClient.describeReplicationTasks(describeReplicationTasksRequestCaptor.capture()))
                .thenReturn(mockDescribeReplicationTasksResult);

        underTest.stopTask(TEST_TASK_ID, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS);

        verifyDescribeReplicationTasksRequestParams(describeReplicationTasksRequestCaptor.getValue());
        verifyNoMoreInteractions(mockDmsClient);
    }

    @Test
    void stopTaskShouldNotFailWhenThereIsNoRunningReplicationTask() {
        List<ReplicationTask> tasks = Collections.emptyList();

        when(mockDescribeReplicationTasksResult.getReplicationTasks()).thenReturn(tasks);
        when(mockDmsClient.describeReplicationTasks(describeReplicationTasksRequestCaptor.capture())).thenReturn(mockDescribeReplicationTasksResult);

        underTest.stopTask(TEST_TASK_ID, WAIT_INTERVAL_SECONDS, MAX_ATTEMPTS);

        verifyDescribeReplicationTasksRequestParams(describeReplicationTasksRequestCaptor.getValue());
        verifyNoMoreInteractions(mockDescribeReplicationTasksResult);
    }

    @Test
    void getTaskStartTimeShouldReturnTheStartTimeOfReplicationTask() {
        Date taskStartTime = new Date();
        List<ReplicationTask> tasks = new ArrayList<>();
        tasks.add(createReplicationTask("stopped").withReplicationTaskStartDate(taskStartTime));

        when(mockDescribeReplicationTasksResult.getReplicationTasks()).thenReturn(tasks);
        when(mockDmsClient.describeReplicationTasks(describeReplicationTasksRequestCaptor.capture()))
                .thenReturn(mockDescribeReplicationTasksResult);

        Date actualTaskStartTime = underTest.getTaskStartTime(TEST_TASK_ID);

        assertThat(actualTaskStartTime, equalTo(taskStartTime));
        verifyDescribeReplicationTasksRequestParams(describeReplicationTasksRequestCaptor.getValue());
        verifyNoMoreInteractions(mockDescribeReplicationTasksResult);
    }

    @Test
    void getTaskStartTimeShouldThrowAnExceptionWhenReplicationTaskStartTimeIsNull() {
        List<ReplicationTask> tasks = new ArrayList<>();
        tasks.add(createReplicationTask("stopped").withReplicationTaskStartDate(null));

        when(mockDescribeReplicationTasksResult.getReplicationTasks()).thenReturn(tasks);
        when(mockDmsClient.describeReplicationTasks(describeReplicationTasksRequestCaptor.capture()))
                .thenReturn(mockDescribeReplicationTasksResult);

        assertThrows(DmsClientException.class, () -> underTest.getTaskStartTime(TEST_TASK_ID));
        verifyDescribeReplicationTasksRequestParams(describeReplicationTasksRequestCaptor.getValue());
        verifyNoMoreInteractions(mockDescribeReplicationTasksResult);
    }

    @Test
    void getTaskStartTimeShouldThrowAnExceptionWhenNoReplicationTaskIsFound() {
        List<ReplicationTask> tasks = Collections.emptyList();

        when(mockDescribeReplicationTasksResult.getReplicationTasks()).thenReturn(tasks);
        when(mockDmsClient.describeReplicationTasks(describeReplicationTasksRequestCaptor.capture()))
                .thenReturn(mockDescribeReplicationTasksResult);

        assertThrows(DmsClientException.class, () -> underTest.getTaskStartTime(TEST_TASK_ID));
        verifyDescribeReplicationTasksRequestParams(describeReplicationTasksRequestCaptor.getValue());
        verifyNoMoreInteractions(mockDescribeReplicationTasksResult);
    }

    @Test
    void getReplicationTaskTableStatisticsShouldReturnListOfTableStatistics() {
        List<ReplicationTask> tasks = new ArrayList<>();
        tasks.add(createReplicationTask("running")
                .withReplicationTaskIdentifier(TEST_TASK_ID)
                .withReplicationTaskArn("replication-task-arn"));
        when(mockDmsClient.describeReplicationTasks(describeReplicationTasksRequestCaptor.capture()))
                .thenReturn(mockDescribeReplicationTasksResult);
        when(mockDescribeReplicationTasksResult.getReplicationTasks()).thenReturn(tasks);


        when(mockDmsClient.describeTableStatistics(describeTableStatisticsRequestCaptor.capture()))
                .thenReturn(mockDescribeTableStatisticsResult);

        List<TableStatistics> expectedTableStatistics = Collections.singletonList(
                new TableStatistics()
                        .withAppliedDeletes(10L)
                        .withDeletes(10L)
        );
        when(mockDescribeTableStatisticsResult.getTableStatistics()).thenReturn(expectedTableStatistics);

        List<TableStatistics> actualTableStatistics = underTest.getReplicationTaskTableStatistics(TEST_TASK_ID);
        assertEquals(expectedTableStatistics, actualTableStatistics);

        verifyDescribeReplicationTasksRequestParams(describeReplicationTasksRequestCaptor.getValue());
        assertEquals("replication-task-arn", describeTableStatisticsRequestCaptor.getValue().getReplicationTaskArn());
        verifyNoMoreInteractions(mockDescribeReplicationTasksResult);
    }

    @Test
    void getReplicationTaskTableStatisticsShouldThrowWhenNoReplicationTaskIsFound() {
        List<ReplicationTask> tasks = Collections.emptyList();

        when(mockDmsClient.describeReplicationTasks(any())).thenReturn(mockDescribeReplicationTasksResult);
        when(mockDescribeReplicationTasksResult.getReplicationTasks()).thenReturn(tasks);

        assertThrows(DmsClientException.class, () -> underTest.getReplicationTaskTableStatistics(TEST_TASK_ID));
    }

    @Test
    void updateCdcTaskStartTimeShouldSetTheStartTimeOfCdcTask() {
        Date cdcStartTime = new Date();

        ReplicationTask cdcReplicationTask = new ReplicationTask().withReplicationTaskArn(TASK_TASK_ARN);
        DescribeReplicationTasksResult describeReplicationTasksResult = new DescribeReplicationTasksResult()
                .withReplicationTasks(Collections.singletonList(cdcReplicationTask));

        when(mockDmsClient.describeReplicationTasks(any())).thenReturn(describeReplicationTasksResult);
        when(mockDmsClient.modifyReplicationTask(modifyReplicationTaskRequestCaptor.capture()))
                .thenReturn(new ModifyReplicationTaskResult());

        underTest.updateCdcTaskStartTime(cdcStartTime, TEST_TASK_ID);

        ModifyReplicationTaskRequest modifyReplicationTaskRequest = modifyReplicationTaskRequestCaptor.getValue();
        assertThat(modifyReplicationTaskRequest.getReplicationTaskArn(), equalTo(TASK_TASK_ARN));
        assertThat(modifyReplicationTaskRequest.getReplicationTaskIdentifier(), equalTo(TEST_TASK_ID));
        assertThat(modifyReplicationTaskRequest.getCdcStartTime(), equalTo(cdcStartTime));
    }

    @Test
    void updateCdcTaskStartTimeShouldThrowExceptionWhenNoReplicationTaskIsFound() {
        Date cdcStartTime = new Date();
        List<ReplicationTask> tasks = Collections.emptyList();

        DescribeReplicationTasksResult describeReplicationTasksResult = new DescribeReplicationTasksResult()
                .withReplicationTasks(tasks);

        when(mockDmsClient.describeReplicationTasks(any())).thenReturn(describeReplicationTasksResult);

        assertThrows(DmsClientException.class, () -> underTest.updateCdcTaskStartTime(cdcStartTime, TEST_TASK_ID));
        verifyNoMoreInteractions(mockDmsClient);
    }

    @Test
    void updateCdcTaskStartTimeShouldThrowExceptionWhenDescribingReplicationTasksFails() {
        Date cdcStartTime = new Date();

        doThrow(new AmazonServiceException("Client exception")).when(mockDmsClient).describeReplicationTasks(any());

        assertThrows(AmazonServiceException.class, () -> underTest.updateCdcTaskStartTime(cdcStartTime, TEST_TASK_ID));
        verifyNoMoreInteractions(mockDmsClient);
    }

    @Test
    void updateCdcTaskStartTimeShouldThrowExceptionWhenModifyingReplicationTaskFails() {
        Date cdcStartTime = new Date();

        ReplicationTask cdcReplicationTask = new ReplicationTask().withReplicationTaskArn(TASK_TASK_ARN);
        DescribeReplicationTasksResult describeReplicationTasksResult = new DescribeReplicationTasksResult()
                .withReplicationTasks(Collections.singletonList(cdcReplicationTask));

        when(mockDmsClient.describeReplicationTasks(any())).thenReturn(describeReplicationTasksResult);
        doThrow(new AmazonServiceException("Client exception")).when(mockDmsClient).modifyReplicationTask(any());

        assertThrows(AmazonServiceException.class, () -> underTest.updateCdcTaskStartTime(cdcStartTime, TEST_TASK_ID));
    }

    @Test
    void getReplicationTaskTablesShouldFailWhenThereIsNoReplicationTaskFound() {
        DescribeReplicationTasksResult dmsTasksResult = new DescribeReplicationTasksResult().withReplicationTasks(Collections.emptyList());

        when(mockDmsClient.describeReplicationTasks(describeReplicationTasksRequestCaptor.capture())).thenReturn(dmsTasksResult);

        assertThrows(DmsClientException.class, () -> underTest.getReplicationTaskTables(DOMAIN_KEY));
        assertFalse(describeReplicationTasksRequestCaptor.getValue().getWithoutSettings());
    }

    @Test
    void getReplicationTaskTablesShouldReturnConfiguredTables() throws JsonProcessingException {
        ObjectNode tableMappings = createRootNode();
        ArrayList<ObjectNode> selections = new ArrayList<>(Arrays.asList(
                createSelection(1, "domain.table_1", "public", "table_1"),
                createSelection(2, "domain.table_2", "public", "TABLE_2")
        ));

        ArrayList<ObjectNode> transformations = new ArrayList<>(Arrays.asList(
                createTableTransformation(3, "convert-lowercase", "%", "%"),
                createSchemaRenameTransformation(4, "schema-rename", "public", "DOMAIN"),
                createDataTypeChangeTransformation(5, "start_time-to-string", "public", "table_1", "start_time", "time"),
                createColumnAdditionTransformation(6, "add-column", "%", "%", "checkpoint_col", "$AR_H_CHANGE_SEQ", "$AR_H_CHANGE_SEQ")
        ));

        tableMappings.set("rules", createRules(selections, transformations));

        ReplicationTask replicationTask = createReplicationTaskWithMappings(tableMappings);
        DescribeReplicationTasksResult dmsTasksResult = new DescribeReplicationTasksResult()
                .withReplicationTasks(Collections.singletonList(replicationTask));

        when(mockDmsClient.describeReplicationTasks(any())).thenReturn(dmsTasksResult);

        ImmutableSet<ImmutablePair<String, String>> tables = underTest.getReplicationTaskTables(DOMAIN_KEY);

        assertThat(tables, containsInAnyOrder(ImmutableSet.of(ImmutablePair.of("domain", "table_1"), ImmutablePair.of("domain", "table_2")).toArray())); // Source and tables are converted to lowercase
    }

    @Test
    void getReplicationTaskTablesShouldFailWhenTableMappingContainsNoSchemaRenameRule() {
        ObjectNode tableMappings = createRootNode();
        ArrayList<ObjectNode> selections = new ArrayList<>(Arrays.asList(
                createSelection(1, "domain.table_1", "public", "table_1"),
                createSelection(2, "domain.table_2", "public", "table_2")
        ));

        ArrayList<ObjectNode> transformations = new ArrayList<>(Collections.singletonList(
                createTableTransformation(3, "convert-lowercase", "%", "%")
        ));

        tableMappings.set("rules", createRules(selections, transformations));

        ReplicationTask replicationTask = createReplicationTaskWithMappings(tableMappings);
        DescribeReplicationTasksResult dmsTasksResult = new DescribeReplicationTasksResult()
                .withReplicationTasks(Collections.singletonList(replicationTask));

        when(mockDmsClient.describeReplicationTasks(any())).thenReturn(dmsTasksResult);

        assertThrows(DmsClientException.class, () -> underTest.getReplicationTaskTables(DOMAIN_KEY));
    }

    @Test
    void getReplicationTaskTablesShouldFailWhenTableMappingContainsNoSelectionRule() {
        ObjectNode tableMappings = createRootNode();
        ArrayList<ObjectNode> selections = new ArrayList<>();

        ArrayList<ObjectNode> transformations = new ArrayList<>(Arrays.asList(
                createTableTransformation(1, "convert-lowercase", "%", "%"),
                createSchemaRenameTransformation(2, "schema-rename", "public", "domain"),
                createDataTypeChangeTransformation(3, "start_time-to-string", "public", "table_1", "start_time", "time"),
                createColumnAdditionTransformation(4, "add-column", "%", "%", "checkpoint_col", "$AR_H_CHANGE_SEQ", "$AR_H_CHANGE_SEQ")
        ));

        tableMappings.set("rules", createRules(selections, transformations));

        ReplicationTask replicationTask = createReplicationTaskWithMappings(tableMappings);
        DescribeReplicationTasksResult dmsTasksResult = new DescribeReplicationTasksResult()
                .withReplicationTasks(Collections.singletonList(replicationTask));

        when(mockDmsClient.describeReplicationTasks(any())).thenReturn(dmsTasksResult);

        assertThrows(DmsClientException.class, () -> underTest.getReplicationTaskTables(DOMAIN_KEY));
    }

    @Test
    void getTaskByDomainShouldRetrieveTaskForEachDomain() {
        ArrayList<String> taskIds = new ArrayList<>(Arrays.asList(
                "dpr-dms-dps-postgres-s3-dps-activities-task-env",
                "dpr-dms-nomis-oracle-s3-finance-txns-cdc-task-env",
                "dpr-dms-nomis-oracle-s3-finance-txns-full-load-env",
                "dpr-dms-nomis-oracle-s3-gangs-task-env",
                "dpr-dms-nomis-oracle-s3-prisoner-address-task-env",
                "dpr-dms-nomis-oracle-s3-prisoner-task-env"
        ));

        List<ReplicationTask> replicationTasks = taskIds.stream()
                .map(taskId -> new ReplicationTask().withReplicationTaskIdentifier(taskId))
                .toList();

        DescribeReplicationTasksResult dmsTasksResult = new DescribeReplicationTasksResult()
                .withReplicationTasks(replicationTasks);

        when(mockDmsClient.describeReplicationTasks(describeReplicationTasksRequestCaptor.capture())).thenReturn(dmsTasksResult);

        verifyRetrievedTask("dps-activities", "dpr-dms-dps-postgres-s3-dps-activities-task-env");
        verifyRetrievedTask("finance-txns", "dpr-dms-nomis-oracle-s3-finance-txns-cdc-task-env");
        verifyRetrievedTask("gangs", "dpr-dms-nomis-oracle-s3-gangs-task-env");
        verifyRetrievedTask("prisoner-address", "dpr-dms-nomis-oracle-s3-prisoner-address-task-env");
        verifyRetrievedTask("prisoner", "dpr-dms-nomis-oracle-s3-prisoner-task-env");

        List<Boolean> booleanStream = describeReplicationTasksRequestCaptor.getAllValues()
                .stream()
                .map(DescribeReplicationTasksRequest::getWithoutSettings)
                .toList();

        assertThat(booleanStream, everyItem(is(false)));
    }

    private void verifyRetrievedTask(String domain, String expected) {
        Optional<ReplicationTask> retrievedTask = underTest.getTaskByDomain(domain);

        assertTrue(retrievedTask.isPresent());
        assertEquals(expected, retrievedTask.get().getReplicationTaskIdentifier());
    }

    private static void verifyDescribeReplicationTasksRequestParams(DescribeReplicationTasksRequest describeReplicationTasksRequest) {
        assertThat(
                new HashSet<>(describeReplicationTasksRequest.getFilters()),
                containsInAnyOrder(new Filter().withName("replication-task-id").withValues(TEST_TASK_ID))
        );
        assertTrue(describeReplicationTasksRequest.getWithoutSettings());
    }

    private static ReplicationTask createReplicationTask(String state) {
        return new ReplicationTask().withStatus(state);
    }

    private static ReplicationTask createReplicationTaskWithMappings(ObjectNode tableMappings) {
        return new ReplicationTask()
                .withReplicationTaskIdentifier("project-dms-source-s3-" + DOMAIN_KEY + "-task-env")
                .withTableMappings(tableMappings.toPrettyString());
    }

    @NotNull
    private static ObjectNode createRootNode() {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.createObjectNode();
    }

    @NotNull
    private static ArrayNode createRules(ArrayList<ObjectNode> selections, ArrayList<ObjectNode> transformations) {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode rules = mapper.createArrayNode();
        selections.forEach(rules::add);
        transformations.forEach(rules::add);

        return rules;
    }

    @NotNull
    private static ObjectNode createSelection(int id, String ruleName, String schemaName, String tableName) {
        ObjectNode selection = createCommonRuleFields(id, ruleName, "selection");
        selection.set("object-locator", createTableObjectLocator(schemaName, tableName));
        selection.put("rule-action", "include");

        return selection;
    }

    @NotNull
    private static ObjectNode createTableTransformation(int id, String ruleName, String schemaName, String tableName) {
        ObjectNode transformation = createCommonRuleFields(id, ruleName, "transformation");
        transformation.set("object-locator", createTableObjectLocator(schemaName, tableName));
        transformation.put("rule-action", "schema-rename");
        transformation.put("rule-target", "table");

        return transformation;
    }

    @NotNull
    private static ObjectNode createSchemaRenameTransformation(int id, String ruleName, String schemaName, String newSchemaName) {
        ObjectNode transformation = createCommonRuleFields(id, ruleName, "transformation");
        transformation.set("object-locator", createSchemaObjectLocator(schemaName));
        transformation.put("rule-action", "rename");
        transformation.put("rule-target", "schema");
        transformation.put("value", newSchemaName);

        return transformation;
    }

    @NotNull
    private static ObjectNode createDataTypeChangeTransformation(int id, String ruleName, String schemaName, String tableName, String columnName, String dataType) {
        ObjectNode transformation = createCommonRuleFields(id, ruleName, "transformation");
        transformation.put("rule-action", "change-data-type");
        transformation.put("rule-target", "column");
        transformation.put("column-name", columnName);
        transformation.put("data-type", dataType);
        transformation.set("object-locator", createTableObjectLocator(schemaName, tableName));
        transformation.set("dataType", createDataType());

        return transformation;
    }

    @NotNull
    private static ObjectNode createColumnAdditionTransformation(int id, String ruleName, String schemaName, String tableName, String columnName, String value, String expr) {
        ObjectNode transformation = createCommonRuleFields(id, ruleName, "transformation");
        transformation.put("rule-action", "change-data-type");
        transformation.put("rule-target", "column");
        transformation.put("column-name", columnName);
        transformation.put("value", value);
        transformation.put("expression", expr);
        transformation.set("object-locator", createTableObjectLocator(schemaName, tableName));
        transformation.set("dataType", createDataType());

        return transformation;
    }

    @NotNull
    private static ObjectNode createTableObjectLocator(String schemaName, String tableName) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode tableObjectLocator = mapper.createObjectNode();
        tableObjectLocator.put("schema-name", schemaName);
        tableObjectLocator.put("table-name", tableName);

        return tableObjectLocator;
    }

    @NotNull
    private static ObjectNode createSchemaObjectLocator(String schemaName) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode schemaObjectLocator = mapper.createObjectNode();
        schemaObjectLocator.put("schema-name", schemaName);

        return schemaObjectLocator;
    }

    @NotNull
    private static ObjectNode createDataType() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode dataType = mapper.createObjectNode();
        dataType.put("type", "string");
        dataType.put("length", "8");

        return dataType;
    }

    @NotNull
    private static ObjectNode createCommonRuleFields(int id, String ruleName, String ruleType) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.put("rule-type", ruleType);
        objectNode.put("rule-id", id);
        objectNode.put("rule-name", ruleName);

        return objectNode;
    }
}
