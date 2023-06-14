package uk.gov.justice.digital.schema;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static uk.gov.justice.digital.test.ResourceLoader.getResource;

public class DomainContractValidationTest {

    private static final String RESOURCE_PATH = "/contracts";

    private static final List<String> contracts = Arrays.asList(
            "reference-contract.avsc"
    );

    private static final Schema.Parser avroSchemaParser = new Schema.Parser();

    @Test
    public void allContractsShouldBeValidAvro() {
        contracts.forEach(contract ->
            assertDoesNotThrow(
                    () -> avroSchemaParser.parse(getResource(RESOURCE_PATH + "/" + contract)),
                    "Schema: " + contract + " should parse as Avro without error"
            )
        );
    }
}
