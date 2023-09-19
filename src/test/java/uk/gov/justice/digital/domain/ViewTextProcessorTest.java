package uk.gov.justice.digital.domain;

import lombok.val;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.common.ColumnMapping;
import uk.gov.justice.digital.common.SourceMapping;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIn.in;
import static org.hamcrest.core.Every.everyItem;

class ViewTextProcessorTest {

    private static final String externalMovements = "nomis.offender_external_movements";
    private static final String movementReasons = "nomis.movement_reasons";
    private static final String agencyLocations = "nomis.agency_locations";
    private static final String originLocation = "origin_location";
    private static final String destinationLocation = "destination_location";

    @Test
    public void shouldCreateColumnMappingForViewTextWithSingleJoinColumn() throws Exception {
        val table1 = "nomis.table_1";
        val table2 = "nomis.table_2";

        val table1Column1 = "table_1_column_1";
        val table2Column1 = "table_2_column_1";

        val expectedTable1To2Mappings = new HashMap<ColumnMapping, ColumnMapping>();
        expectedTable1To2Mappings.put(
                ColumnMapping.create(table1, table1Column1),
                ColumnMapping.create(table2, table2Column1)
        );

        val expectedTable2To1Mappings = new HashMap<ColumnMapping, ColumnMapping>();
        expectedTable2To1Mappings.put(
                ColumnMapping.create(table2, table2Column1),
                ColumnMapping.create(table1, table1Column1)
        );

        val viewText = "select " +
                table1 + ".offender_book_id as id, " +
                table2 + ".birth_date as birth_date, " +
                table1 + ".living_unit_id as living_unit_id, " +
                table2 + ".first_name as first_name, " +
                table2 + ".last_name as last_name, " +
                table2 + ".offender_id_display as offender_no " +
                "from " + table1 + " " +
                "join " + table2 + " " +
                "on " + table1 + "." + table1Column1 + " = " + table2 + "." + table2Column1;

        SourceMapping table1To2mappings = ViewTextProcessor.buildSourceMapping(table1, table2, viewText);
        SourceMapping table2To1Mappings = ViewTextProcessor.buildSourceMapping(table2, table1, viewText);

        assertThat(
                table1To2mappings.getColumnMap().entrySet(),
                everyItem(is(in(expectedTable1To2Mappings.entrySet())))
        );

        assertThat(
                table2To1Mappings.getColumnMap().entrySet(),
                everyItem(is(in(expectedTable2To1Mappings.entrySet())))
        );
    }

    @Test
    public void shouldCreateColumnMappingForViewTextWithMultipleJoinColumns() throws Exception {
        val table1 = "nomis.table_1";
        val table2 = "nomis.table_2";

        val table1Column1 = "table_1_column_1";
        val table2Column1 = "table_2_column_1";

        val table1Column2 = "table_1_column_2";
        val table2Column2 = "table_2_column_2";

        val expectedTable1To2Mappings = new HashMap<ColumnMapping, ColumnMapping>();
        expectedTable1To2Mappings.put(
                ColumnMapping.create(table1, table1Column1),
                ColumnMapping.create(table2, table2Column1)
        );
        expectedTable1To2Mappings.put(
                ColumnMapping.create(table1, table1Column2),
                ColumnMapping.create(table2, table2Column2)
        );

        val expectedTable2To1Mappings = new HashMap<ColumnMapping, ColumnMapping>();
        expectedTable2To1Mappings.put(
                ColumnMapping.create(table2, table2Column1),
                ColumnMapping.create(table1, table1Column1)
        );
        expectedTable2To1Mappings.put(
                ColumnMapping.create(table2, table2Column2),
                ColumnMapping.create(table1, table1Column2)
        );

        val viewText = "select " +
                table1 + ".offender_book_id as id, " +
                table2 + ".birth_date as birth_date, " +
                table1 + ".living_unit_id as living_unit_id, " +
                table2 + ".first_name as first_name, " +
                table2 + ".last_name as last_name, " +
                table2 + ".offender_id_display as offender_no " +
                "from " + table1 + " " +
                "join " + table2 + " " +
                "on " + table1 + "." + table1Column1 + " = " + table2 + "." + table2Column1 + " " +
                "and " + table2 + "." + table2Column2 + " = " + table1 + "." + table1Column2;

        SourceMapping table1To2Mappings = ViewTextProcessor.buildSourceMapping(table1, table2, viewText);
        SourceMapping table2To1Mappings = ViewTextProcessor.buildSourceMapping(table2, table1, viewText);

        assertThat(
                table1To2Mappings.getColumnMap().entrySet(),
                everyItem(is(in(expectedTable1To2Mappings.entrySet())))
        );

        assertThat(
                table2To1Mappings.getColumnMap().entrySet(),
                everyItem(is(in(expectedTable2To1Mappings.entrySet())))
        );
    }

    @Test
    public void shouldCreateColumnMappingForViewTextWithMultipleJoinExpressions() throws Exception {
        val table1 = "nomis.table_1";
        val table2 = "nomis.table_2";
        val table3 = "nomis.table_3";
        val table4 = "nomis.table_4";
        val table5 = "nomis.table_5";

        val table1Column1 = "table_1_column_1";
        val table2Column1 = "table_2_column_1";

        val table1Column2 = "table_1_column_2";
        val table2Column2 = "table_2_column_2";

        val table1Column3 = "table_1_column_3";
        val table3Column3 = "table_3_column_3";

        val table1Column4 = "table_1_column_4";
        val table4Column4 = "table_4_column_4";

        val table1Column5 = "table_1_column_5";
        val table5Column5 = "table_5_column_5";

        val expectedTable1To2Mappings = new HashMap<ColumnMapping, ColumnMapping>();
        expectedTable1To2Mappings.put(
                ColumnMapping.create(table1, table1Column1),
                ColumnMapping.create(table2, table2Column1)
        );
        expectedTable1To2Mappings.put(
                ColumnMapping.create(table1, table1Column2),
                ColumnMapping.create(table2, table2Column2)
        );

        val expectedTable1To3Mappings = new HashMap<ColumnMapping, ColumnMapping>();
        expectedTable1To3Mappings.put(
                ColumnMapping.create(table1, table1Column3),
                ColumnMapping.create(table3, table3Column3)
        );

        val expectedTable1To4Mappings = new HashMap<ColumnMapping, ColumnMapping>();
        expectedTable1To4Mappings.put(
                ColumnMapping.create(table1, table1Column4),
                ColumnMapping.create(table4, table4Column4)
        );

        val expectedTable1To5Mappings = new HashMap<ColumnMapping, ColumnMapping>();
        expectedTable1To5Mappings.put(
                ColumnMapping.create(table1, table1Column5),
                ColumnMapping.create(table5, table5Column5)
        );

        val viewText = "select " +
                table1 + ".offender_book_id as id, " +
                table2 + ".birth_date as birth_date, " +
                table1 + ".living_unit_id as living_unit_id, " +
                table2 + ".first_name as first_name, " +
                table2 + ".last_name as last_name, " +
                table2 + ".offender_id_display as offender_no " +
                "from " + table1 + " " +
                "join " + table2 + " " +
                "on " + table1 + "." + table1Column1 + " = " + table2 + "." + table2Column1 + " " +
                "and " + table2 + "." + table2Column2 + " = " + table1 + "." + table1Column2 + " " +
                "LEFT JOIN " + table3 + " " +
                "on " + table1 + "." + table1Column3 + " = " + table3 + "." + table3Column3 + " " +
                "RIGHT  JOIN " + table4 + " " +
                "on " + table1 + "." + table1Column4 + " = " + table4 + "." + table4Column4 + " " +
                "full join " + table5 + " " +
                "on " + table1 + "." + table1Column5 + " = " + table5 + "." + table5Column5;

        SourceMapping table1To2Mappings = ViewTextProcessor.buildSourceMapping(table1, table2, viewText);
        SourceMapping table1To3Mappings = ViewTextProcessor.buildSourceMapping(table1, table3, viewText);
        SourceMapping table1To4Mappings = ViewTextProcessor.buildSourceMapping(table1, table4, viewText);
        SourceMapping table1To5Mappings = ViewTextProcessor.buildSourceMapping(table1, table5, viewText);

        assertThat(
                table1To2Mappings.getColumnMap().entrySet(),
                everyItem(is(in(expectedTable1To2Mappings.entrySet())))
        );

        assertThat(
                table1To3Mappings.getColumnMap().entrySet(),
                everyItem(is(in(expectedTable1To3Mappings.entrySet())))
        );

        assertThat(
                table1To4Mappings.getColumnMap().entrySet(),
                everyItem(is(in(expectedTable1To4Mappings.entrySet())))
        );

        assertThat(
                table1To5Mappings.getColumnMap().entrySet(),
                everyItem(is(in(expectedTable1To5Mappings.entrySet())))
        );
    }

    @Test
    public void shouldResolveAliasesWhenBuildingColumnMappings() throws Exception {
        val table1 = "nomis.table_1";
        val table2 = "nomis.table_2";
        val table3 = "nomis.table_3";

        val table2Alias = "table2_alias";

        val table1Column1 = "table_1_column_1";
        val table2Column1 = "table_2_column_1";
        val table3Column1 = "table_3_column_1";

        val expectedMappings = new HashMap<ColumnMapping, ColumnMapping>();
        expectedMappings.put(
                ColumnMapping.create(table1, table1Column1),
                ColumnMapping.create(table2Alias, table2Column1)
        );

        val viewText = "select " +
                table1 + ".offender_book_id as id, " +
                table2 + ".birth_date as birth_date, " +
                table1 + ".living_unit_id as living_unit_id, " +
                table2 + ".first_name as first_name, " +
                table2 + ".last_name as last_name, " +
                table2 + ".offender_id_display as offender_no " +
                "from " + table1 + " " +
                "join " + table2 + " as  " + table2Alias + " " +
                "on " + table1 + "." + table1Column1 + " = table2_alias." + table2Column1 + " " +
                "left join " + table3 + "  as table3_alias " +
                "on " + table1 + "." + table1Column1 + " = table3_alias." + table3Column1;

        val mappings = ViewTextProcessor.buildSourceMapping(table1, table2, viewText);

        assertThat(
                mappings.getColumnMap().entrySet(),
                everyItem(is(in(expectedMappings.entrySet())))
        );
    }

    @Test
    public void shouldReturnEmptyMapForViewTextWithNoJoinCondition() throws Exception {
        val table1 = "nomis.table_1";
        val expectedMappings = new HashMap<ColumnMapping, ColumnMapping>();

        val viewText = "select " +
                table1 + ".offender_book_id as id, " +
                table1 + ".living_unit_id as living_unit_id, " +
                "from " + table1;

        val mappings = ViewTextProcessor.buildSourceMapping(table1, table1, viewText);

        assertThat(
                mappings.getColumnMap().entrySet(),
                everyItem(is(in(expectedMappings.entrySet())))
        );
    }

    @Test
    public void shouldResolveAliasesAndBuildColumnMappingsForComplexQuery() throws Exception {

        val expectedTable1To2Mappings = new HashMap<ColumnMapping, ColumnMapping>();
        expectedTable1To2Mappings.put(
                ColumnMapping.create(externalMovements, "movement_type"),
                ColumnMapping.create(movementReasons, "movement_type")
        );
        expectedTable1To2Mappings.put(
                ColumnMapping.create(externalMovements, "movement_reason_code"),
                ColumnMapping.create(movementReasons, "movement_reason_code")
        );

        val expectedTable1To3Mappings = new HashMap<ColumnMapping, ColumnMapping>();
        expectedTable1To3Mappings.put(
                ColumnMapping.create(externalMovements, "from_agy_loc_id"),
                ColumnMapping.create(originLocation, "agy_loc_id")
        );
        expectedTable1To3Mappings.put(
                ColumnMapping.create(externalMovements, "to_agy_loc_id"),
                ColumnMapping.create(destinationLocation, "agy_loc_id")
        );

        val expectedTable2To1Mappings = new HashMap<ColumnMapping, ColumnMapping>();
        expectedTable2To1Mappings.put(
                ColumnMapping.create(movementReasons, "movement_type"),
                ColumnMapping.create(externalMovements, "movement_type")
        );
        expectedTable2To1Mappings.put(
                ColumnMapping.create(movementReasons, "movement_reason_code"),
                ColumnMapping.create(externalMovements, "movement_reason_code")
        );

        val expectedTable2To3Mappings = new HashMap<ColumnMapping, ColumnMapping>();

        val expectedTable3To1Mappings = new HashMap<ColumnMapping, ColumnMapping>();
        expectedTable3To1Mappings.put(
                ColumnMapping.create(originLocation, "agy_loc_id"),
                ColumnMapping.create(externalMovements, "from_agy_loc_id")
        );
        expectedTable3To1Mappings.put(
                ColumnMapping.create(destinationLocation, "agy_loc_id"),
                ColumnMapping.create(externalMovements, "to_agy_loc_id")
        );

        val expectedTable3To2Mappings = new HashMap<ColumnMapping, ColumnMapping>();

        val viewText = complexViewTextWithAliases();

        SourceMapping table1To2Mappings = ViewTextProcessor.buildSourceMapping(externalMovements, movementReasons, viewText);
        SourceMapping table1To3Mappings = ViewTextProcessor.buildSourceMapping(externalMovements, agencyLocations, viewText);
        SourceMapping table2To1Mappings = ViewTextProcessor.buildSourceMapping(movementReasons, externalMovements, viewText);
        SourceMapping table2To3Mappings = ViewTextProcessor.buildSourceMapping(movementReasons, agencyLocations, viewText);
        SourceMapping table3To1Mappings = ViewTextProcessor.buildSourceMapping(agencyLocations, externalMovements, viewText);
        SourceMapping table3To2Mappings = ViewTextProcessor.buildSourceMapping(agencyLocations, movementReasons, viewText);

        assertThat(
                table1To2Mappings.getColumnMap().entrySet(),
                everyItem(is(in(expectedTable1To2Mappings.entrySet())))
        );

        assertThat(
                table1To3Mappings.getColumnMap().entrySet(),
                everyItem(is(in(expectedTable1To3Mappings.entrySet())))
        );

        assertThat(
                table2To1Mappings.getColumnMap().entrySet(),
                everyItem(is(in(expectedTable2To1Mappings.entrySet())))
        );

        assertThat(
                table2To3Mappings.getColumnMap().entrySet(),
                everyItem(is(in(expectedTable2To3Mappings.entrySet())))
        );

        assertThat(
                table3To1Mappings.getColumnMap().entrySet(),
                everyItem(is(in(expectedTable3To1Mappings.entrySet())))
        );

        assertThat(
                table3To2Mappings.getColumnMap().entrySet(),
                everyItem(is(in(expectedTable3To2Mappings.entrySet())))
        );
    }

    @Test
    public void shouldBuildAllSourceMappingsInViewText() {
        val tableAliases = new HashMap<String, String>();
        tableAliases.put(originLocation, agencyLocations);
        tableAliases.put(destinationLocation, agencyLocations);

        val sources = Arrays.asList(externalMovements, movementReasons, agencyLocations);

        val expectedTable1To2Mappings = new HashMap<ColumnMapping, ColumnMapping>();
        expectedTable1To2Mappings.put(
                ColumnMapping.create(externalMovements, "movement_type"),
                ColumnMapping.create(movementReasons, "movement_type")
        );
        expectedTable1To2Mappings.put(
                ColumnMapping.create(externalMovements, "movement_reason_code"),
                ColumnMapping.create(movementReasons, "movement_reason_code")
        );

        val expectedTable1To3Mappings = new HashMap<ColumnMapping, ColumnMapping>();
        expectedTable1To3Mappings.put(
                ColumnMapping.create(externalMovements, "from_agy_loc_id"),
                ColumnMapping.create(originLocation, "agy_loc_id")
        );
        expectedTable1To3Mappings.put(
                ColumnMapping.create(externalMovements, "to_agy_loc_id"),
                ColumnMapping.create(destinationLocation, "agy_loc_id")
        );

        val expectedTable2To1Mappings = new HashMap<ColumnMapping, ColumnMapping>();
        expectedTable2To1Mappings.put(
                ColumnMapping.create(movementReasons, "movement_type"),
                ColumnMapping.create(externalMovements, "movement_type")
        );
        expectedTable2To1Mappings.put(
                ColumnMapping.create(movementReasons, "movement_reason_code"),
                ColumnMapping.create(externalMovements, "movement_reason_code")
        );

        val expectedTable3To1Mappings = new HashMap<ColumnMapping, ColumnMapping>();
        expectedTable3To1Mappings.put(
                ColumnMapping.create(originLocation, "agy_loc_id"),
                ColumnMapping.create(externalMovements, "from_agy_loc_id")
        );
        expectedTable3To1Mappings.put(
                ColumnMapping.create(destinationLocation, "agy_loc_id"),
                ColumnMapping.create(externalMovements, "to_agy_loc_id")
        );

        val expectedSourceMapping = new HashSet<SourceMapping>();
        expectedSourceMapping.add(SourceMapping.create(externalMovements, movementReasons, Collections.emptyMap(), expectedTable1To2Mappings));
        expectedSourceMapping.add(SourceMapping.create(externalMovements, agencyLocations, tableAliases, expectedTable1To3Mappings));
        expectedSourceMapping.add(SourceMapping.create(movementReasons, externalMovements, Collections.emptyMap(), expectedTable2To1Mappings));
        expectedSourceMapping.add(SourceMapping.create(agencyLocations, externalMovements, tableAliases, expectedTable3To1Mappings));

        val viewText = complexViewTextWithAliases();

        val sourceMappings = ViewTextProcessor.buildAllSourceMappings(sources, viewText);

        assertThat(sourceMappings, everyItem(is(in(expectedSourceMapping))));
    }

    @NotNull
    private static String complexViewTextWithAliases() {
        return "select concat(cast(" + externalMovements + ".offender_book_id as string), '.', cast(" + externalMovements + ".movement_seq as string)) as id, " +
                externalMovements + ".offender_book_id as prisoner, " +
                externalMovements + ".movement_date as date, " +
                externalMovements + ".movement_time as time, " +
                externalMovements + ".direction_code as direction, " +
                externalMovements + ".movement_type as type, " +
                "origin_location.description as origin, " +
                "destination_location.description as destination, " +
                movementReasons + ".description as reason " +
                "from " + externalMovements + " " +
                "join " + movementReasons + " " +
                "on " + movementReasons + ".movement_type=" + externalMovements + ".movement_type " +
                "and " + movementReasons + ".movement_reason_code=" + externalMovements + ".movement_reason_code " +
                "left join " + agencyLocations + " as origin_location " +
                "on " + externalMovements + ".from_agy_loc_id = origin_location.agy_loc_id " +
                "left join " + agencyLocations + " as destination_location " +
                "on " + externalMovements + ".to_agy_loc_id = destination_location.agy_loc_id";
    }
}