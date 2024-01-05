package uk.gov.justice.digital.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import uk.gov.justice.digital.client.s3.S3SchemaClient;
import uk.gov.justice.digital.converter.avro.AvroToSparkSchemaConverter;
import uk.gov.justice.digital.datahub.model.SourceReference;

import java.util.*;
import java.util.stream.Collectors;

@Singleton
public class SourceReferenceService {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final S3SchemaClient schemaClient;
    private final AvroToSparkSchemaConverter converter;

    @Inject
    public SourceReferenceService(S3SchemaClient schemaClient,
                                  AvroToSparkSchemaConverter converter) {
        this.schemaClient = schemaClient;
        this.converter = converter;
    }

    public Optional<SourceReference> getSourceReference(String source, String table) {
        val key = generateKey(source, table);
        return schemaClient.getSchema(key).map(this::createFromAvroSchema);
    }

    public List<SourceReference> getAllSourceReferences(ImmutableSet<ImmutablePair<String, String>> schemaGroup) {
        return schemaClient.getAllSchemas(schemaGroup)
                .stream()
                .map(this::createFromAvroSchema)
                .collect(Collectors.toList());
    }

    private String generateKey(String source, String table) {
        return String.join("/", source, table).toLowerCase();
    }

    private SourceReference createFromAvroSchema(S3SchemaClient.S3SchemaResponse response) {
        val parsedAvro = parseAvroString(response.getAvro());

        return new SourceReference(
                response.getId(),
                parsedAvro.getService(),
                parsedAvro.getNameWithoutVersionSuffix(),
                parsedAvro.findPrimaryKey()
                        .orElseThrow(() -> new IllegalStateException("No primary key found in schema: " + response)),
                response.getVersionId(),
                converter.convert(response.getAvro())
        );

    }

    private AvroSchema parseAvroString(String avro) {
        try {
            return objectMapper.readValue(avro, AvroSchema.class);
        }
        catch (Exception e) {
            throw new RuntimeException("Caught exception when parsing avro schema", e);
        }
    }

    @Builder
    @Jacksonized
    // Private class that declares only the fields we are interested in when constructing a SourceReference.
    private static class AvroSchema {
        @Getter
        private final String service;
        private final String name;
        private final List<Map<String, Object>> fields;

        private boolean isPrimaryKey(Map<String, Object> attributes) {
            return attributes.containsKey("key") &&
                    Optional.ofNullable(attributes.get("key"))
                            .flatMap(this::castToString)
                            .filter(v -> v.equals("primary"))
                            .isPresent();
        }

        private Optional<String> castToString(Object o) {
            return Optional.ofNullable(o)
                    .filter(String.class::isInstance)
                    .map(String.class::cast);
        }

        public Optional<SourceReference.PrimaryKey> findPrimaryKey() {
            return fields.stream()
                    .filter(this::isPrimaryKey)
                    .map(f -> f.get("name"))
                    .collect(Collectors.collectingAndThen(Collectors.toList(), this::createPrimaryKey));
        }

        private Optional<SourceReference.PrimaryKey> createPrimaryKey(List<Object> list) {
            if(list != null && !list.isEmpty()) {
                return Optional.of(new SourceReference.PrimaryKey(list));
            }
            return Optional.empty();
        }

        // If the table name has a version suffix e.g. SOME_TABLE_NAME_16 this must be removed from the value used in
        // the source reference instance. See SourceReferenceServiceTest for more context.
        public String getNameWithoutVersionSuffix() {
            return name.replaceFirst("_\\d+", "")
                    .toLowerCase(Locale.ENGLISH);
        }
    }


}
