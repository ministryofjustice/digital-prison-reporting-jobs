package uk.gov.justice.digital.client.dms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.databasemigration.DatabaseMigrationClient;
import software.amazon.awssdk.services.databasemigration.model.ReplicationTask;
import software.amazon.awssdk.services.databasemigration.model.StopReplicationTaskRequest;
import software.amazon.awssdk.services.databasemigration.model.ModifyReplicationTaskRequest;
import software.amazon.awssdk.services.databasemigration.model.TableStatistics;
import software.amazon.awssdk.services.databasemigration.model.DescribeTableStatisticsRequest;
import software.amazon.awssdk.services.databasemigration.model.DescribeTableStatisticsResponse;
import software.amazon.awssdk.services.databasemigration.model.DescribeReplicationTasksRequest;
import software.amazon.awssdk.services.databasemigration.model.Filter;
import uk.gov.justice.digital.exception.DmsClientException;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Singleton
public class DefaultDmsClient {

    private static final Logger logger = LoggerFactory.getLogger(DefaultDmsClient.class);

    private final DatabaseMigrationClient awsDms;

    @Inject
    public DefaultDmsClient(DmsClientProvider dmsClientProvider) {
        this.awsDms = dmsClientProvider.getClient();
    }

    public void stopTask(String taskId, int waitIntervalSeconds, int maxAttempts) {
        Optional<ReplicationTask> optionalTask = getTask(taskId);
        optionalTask.ifPresent(task -> {
                    StopReplicationTaskRequest stopReplicationTaskRequest = StopReplicationTaskRequest
                            .builder()
                            .replicationTaskArn(task.replicationTaskArn())
                            .build();

                    if (task.status().equalsIgnoreCase("running")) {
                        logger.info("Stopping replication task {}", taskId);
                        awsDms.stopReplicationTask(stopReplicationTaskRequest);

                        try {
                            ensureState(taskId, "stopped", waitIntervalSeconds, maxAttempts);
                        } catch (InterruptedException e) {
                            logger.error("Error while ensuring task {} has stopped", taskId, e);
                            Thread.currentThread().interrupt();
                        }
                    } else {
                        logger.info("Replication task {} is not running", taskId);
                    }
                }
        );
    }

    public void updateCdcTaskStartTime(Instant cdcStartTime, String cdcTaskId) {
        Optional<ReplicationTask> optionalTask = getTask(cdcTaskId);

        if (optionalTask.isPresent()) {
            ReplicationTask cdcTask = optionalTask.get();

            logger.info("Modifying replication task");
            val modifyReplicationTaskRequest = ModifyReplicationTaskRequest
                    .builder()
                    .replicationTaskIdentifier(cdcTaskId)
                    .replicationTaskArn(cdcTask.replicationTaskArn())
                    .cdcStartTime(cdcStartTime)
                    .build();

            awsDms.modifyReplicationTask(modifyReplicationTaskRequest);
            logger.info("Modified replication task");
        } else {
            throw new DmsClientException("Failed to get DMS task with Id: " + cdcTaskId);
        }
    }

    public Instant getTaskStartTime(String taskId) {
        Optional<ReplicationTask> optionalTask = getTask(taskId);
        if (optionalTask.isPresent()) {
            return Optional.ofNullable(optionalTask.get().replicationTaskStartDate())
                    .orElseThrow(() -> new DmsClientException("Start time was null for DMS task with Id: " + taskId));
        } else {
            throw new DmsClientException("Failed to get DMS task with Id: " + taskId);
        }
    }

    public List<TableStatistics> getReplicationTaskTableStatistics(String taskId) {
        Optional<ReplicationTask> optionalTask = getTask(taskId);
        ReplicationTask replicationTask = optionalTask.orElseThrow(() ->
                new DmsClientException("Replication task with Id: " + taskId + " not found")
        );
        DescribeTableStatisticsRequest request = DescribeTableStatisticsRequest
                .builder()
                .replicationTaskArn(replicationTask.replicationTaskArn())
                .build();

        DescribeTableStatisticsResponse response = awsDms.describeTableStatistics(request);
        return response.tableStatistics();
    }

    public ImmutableSet<ImmutablePair<String, String>> getReplicationTaskTables(String domainKey) throws JsonProcessingException {
        Optional<ReplicationTask> optionalTask = getTaskByDomain(domainKey);
        ReplicationTask replicationTask = optionalTask.orElseThrow(() ->
                new DmsClientException("Replication task for domain " + domainKey + " not found")
        );

        final Set<String> sources = new HashSet<>();
        final Set<String> tableNames = new HashSet<>();

        String tableMappingsAsString = replicationTask.tableMappings();
        JsonNode tableMappings = new ObjectMapper().readTree(tableMappingsAsString);
        Iterator<JsonNode> ruleElements = tableMappings.get("rules").elements();

        ruleElements.forEachRemaining(valueNode -> {
                    boolean hasRuleActionAndValueFields = valueNode.has("rule-action") && valueNode.has("value");
                    if (hasRuleActionAndValueFields && valueNode.get("rule-action").asText().equalsIgnoreCase("rename")) {
                        sources.add(valueNode.get("value").asText().toLowerCase());
                    }

                    boolean hasRuleTypeAndObjectLocatorFields = valueNode.has("rule-type") && valueNode.has("object-locator");
                    if (hasRuleTypeAndObjectLocatorFields && valueNode.get("rule-type").asText().equalsIgnoreCase("selection")) {
                        tableNames.add(valueNode.get("object-locator").get("table-name").asText().toLowerCase());
                    }
                }
        );

        if (tableNames.isEmpty()) {
            throw new DmsClientException("Exception when retrieving table names for DMS task in domain " + domainKey);
        }

        String source = sources.stream().findFirst()
                .orElseThrow(() -> new DmsClientException("Exception when retrieving schema source for DMS task in domain " + domainKey));

        return ImmutableSet.copyOf(tableNames.stream().map(tableName -> ImmutablePair.of(source, tableName)).collect(Collectors.toSet()));
    }

    private void ensureState(String taskId, String state, int waitIntervalSeconds, int maxAttempts) throws InterruptedException {
        for (int attempts = 0; attempts < maxAttempts; attempts++) {
            logger.info("Ensuring replication task {} is in {} state. Attempt {}", taskId, state, attempts);
            Optional<ReplicationTask> optionalTask = getTask(taskId);
            if (optionalTask.isPresent() && optionalTask.get().status().equalsIgnoreCase(state)) {
                return;
            }

            TimeUnit.SECONDS.sleep(waitIntervalSeconds);
        }

        String errorMessage = String.format("Exhausted attempts waiting for replication task %s to be %s", taskId, state);
        throw new DmsClientException(errorMessage);
    }

    public Optional<ReplicationTask> getTask(String taskId) {
        logger.info("Retrieving replication task {}", taskId);
        val describeReplicationTasksRequest = DescribeReplicationTasksRequest
                .builder()
                .withoutSettings(true)
                .filters(Filter.builder().name("replication-task-id").values(taskId).build())
                .build();

        return awsDms.describeReplicationTasks(describeReplicationTasksRequest)
                .replicationTasks()
                .stream()
                .findFirst();
    }

    public Optional<ReplicationTask> getTaskByDomain(String domain) {
        logger.info("Retrieving replication task for domain {}", domain);
        val describeReplicationTasksRequest = DescribeReplicationTasksRequest
                .builder()
                .withoutSettings(false)
                .build();

        return awsDms.describeReplicationTasks(describeReplicationTasksRequest)
                .replicationTasks()
                .stream()
                .filter(task -> task.replicationTaskIdentifier().matches("(^.+s3-)(" + domain + ")(-cdc-task|-task)(.+)"))
                .findFirst();
    }
}
