package uk.gov.justice.digital.domain;

import lombok.val;
import org.apache.hadoop.shaded.com.google.re2j.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import uk.gov.justice.digital.common.ColumnMapping;
import uk.gov.justice.digital.common.SourceMapping;
import uk.gov.justice.digital.exception.DomainServiceException;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ViewTextProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ViewTextProcessor.class);

    private static final Pattern aliasRegexPattern = Pattern.compile(" (\\S+)(\\s+as\\s+)(\\S+) ");
    private static final Pattern joinExprRegexPattern = Pattern
            .compile("((join)|((left)(\\s+)(join))|((right)(\\s+)(join))|((full)(\\s+)(join)))");

    public static Set<SourceMapping> buildAllSourceMappings(List<String> sources, String viewText) {
        val sourceMappings = new HashSet<SourceMapping>();

        sources.stream()
                .flatMap(source1 -> sources.stream().map(source2 -> Tuple2.apply(source1, source2)))
                .filter(combination -> !combination._1.equalsIgnoreCase(combination._2))
                .forEach(sourcesCombination -> {
                    try {
                        val sourceMapping = ViewTextProcessor.buildSourceMapping(sourcesCombination._1, sourcesCombination._2, viewText);
                        if (!sourceMapping.getColumnMap().isEmpty()) sourceMappings.add(sourceMapping);
                    } catch (DomainServiceException e) {
                        logger.warn("Failed to build source mappings", e);
                        throw new RuntimeException(e);
                    }
                });

        return sourceMappings;
    }

    public static SourceMapping buildSourceMapping(
            String referenceTableName,
            String adjoiningTableName,
            String viewText
    ) throws DomainServiceException {
        val columnMappings = new HashMap<ColumnMapping, ColumnMapping>();

        val fromExpression = viewText.toLowerCase().split(" from ", 2)[1];
        val tableAliases = resolveTableAliases(fromExpression, referenceTableName, adjoiningTableName);
        val joinExpressions = fromExpression.split(joinExprRegexPattern.pattern());

        for (String joinExpression : joinExpressions) {
            val onExpressions = joinExpression.split(" on ");

            if (onExpressions.length == 2) {
                // ViewText has a join expression
                val joinClause = onExpressions[1];
                val andExpressions = Arrays
                        .stream(joinClause.split(" and "))
                        .map(String::trim)
                        .filter(andExpression -> containsTableOrItsAlias(referenceTableName, tableAliases, andExpression) &&
                                containsTableOrItsAlias(adjoiningTableName, tableAliases, andExpression)
                        ).collect(Collectors.toList());

                val joinColumns = andExpressions
                        .stream()
                        .flatMap(str -> Arrays.stream(str.trim().split("=")).map(String::trim))
                        .collect(Collectors.toList());

                val currentTableJoinColumns = joinColumns
                        .stream()
                        .filter(column -> containsTableOrItsAlias(referenceTableName, tableAliases, column))
                        .map(ViewTextProcessor::toColumnMapping)
                        .collect(Collectors.toList());

                val otherTableJoinColumns = joinColumns
                        .stream()
                        .filter(column -> !containsTableOrItsAlias(referenceTableName, tableAliases, column))
                        .map(ViewTextProcessor::toColumnMapping)
                        .collect(Collectors.toList());

                if (currentTableJoinColumns.size() == otherTableJoinColumns.size()) {
                    val intermediateMap = IntStream.range(0, currentTableJoinColumns.size())
                            .boxed()
                            .map(i -> Collections.singletonMap(currentTableJoinColumns.get(i), otherTableJoinColumns.get(i)))
                            .flatMap(map -> map.entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                    columnMappings.putAll(intermediateMap);
                } else {
                    throw new DomainServiceException("Join expression length mismatch in viewText " + viewText);
                }
            }
        }

        return SourceMapping.create(referenceTableName, adjoiningTableName, tableAliases, columnMappings);
    }

    private static HashMap<String, String> resolveTableAliases(String sqlText, String referenceTableName, String adjoiningTableName) {
        val tableAliases = new HashMap<String, String>();
        val matcher = aliasRegexPattern.matcher(sqlText);

        while (matcher.find()) {
            val originalName = matcher.group(1).toLowerCase();
            val alias = matcher.group(3);
            if (originalName.equalsIgnoreCase(referenceTableName) || originalName.equalsIgnoreCase(adjoiningTableName)) {
                tableAliases.put(alias, originalName);
            }
        }

        return tableAliases;
    }

    private static boolean containsTableOrItsAlias(String tableName, HashMap<String, String> tableAliases, String str) {
        if (tableAliases.containsValue(tableName)) {
            val aliases = tableAliases.entrySet().stream()
                    .filter(entrySet -> entrySet.getValue().equalsIgnoreCase(tableName))
                    .map(Map.Entry::getKey).collect(Collectors.toSet());
            return aliases.stream().anyMatch(alias -> str.contains(alias.toLowerCase()));
        } else {
            return str.contains(tableName.toLowerCase());
        }
    }

    private static ColumnMapping toColumnMapping(String column) {
        val tableName = column.substring(0, column.lastIndexOf("."));
        val columnName = column.substring(column.lastIndexOf(".") + 1);
        return ColumnMapping.create(tableName, columnName);
    }

    // Private constructor to prevent instantiation.
    private ViewTextProcessor() {}
}
