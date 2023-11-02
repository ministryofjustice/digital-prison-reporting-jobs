package uk.gov.justice.digital.test;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.Operation.*;
import static uk.gov.justice.digital.test.Fixtures.*;

public class ZoneFixtures {
    // input records
    public static final Row record1Load = RowFactory.create(
            "3",
            RECORD_KEY_1,
            TABLE_SOURCE,
            TABLE_NAME,
            Load.getName(),
            ROW_CONVERTER,
            recordData1,
            recordData1,
            GENERIC_METADATA
    );
    public static final Row record2Load = RowFactory.create(
            "1",
            RECORD_KEY_2,
            TABLE_SOURCE,
            TABLE_NAME,
            Load.getName(),
            ROW_CONVERTER,
            recordData2,
            recordData2,
            GENERIC_METADATA
    );
    public static final Row record3Load = RowFactory.create(
            "2",
            RECORD_KEY_3,
            TABLE_SOURCE,
            TABLE_NAME,
            Load.getName(),
            ROW_CONVERTER,
            recordData3,
            recordData3,
            GENERIC_METADATA
    );
    public static final Row record4Insert = RowFactory.create(
            "0",
            RECORD_KEY_4,
            TABLE_SOURCE,
            TABLE_NAME,
            Insert.getName(),
            ROW_CONVERTER,
            recordData4,
            recordData4,
            GENERIC_METADATA
    );
    public static final Row record5Insert = RowFactory.create(
            "4",
            RECORD_KEY_5,
            TABLE_SOURCE,
            TABLE_NAME,
            Insert.getName(),
            ROW_CONVERTER,
            recordData5,
            recordData5,
            GENERIC_METADATA
    );
    public static final Row record6Insert = RowFactory.create(
            "5",
            RECORD_KEY_6,
            TABLE_SOURCE,
            TABLE_NAME,
            Insert.getName(),
            ROW_CONVERTER,
            recordData6,
            recordData6,
            GENERIC_METADATA
    );
    public static final Row record7Update = RowFactory.create(
            "6",
            RECORD_KEY_7,
            TABLE_SOURCE,
            TABLE_NAME,
            Update.getName(),
            ROW_CONVERTER,
            recordData7,
            recordData7,
            GENERIC_METADATA
    );
    public static final Row record6Deletion = RowFactory.create(
            "7",
            RECORD_KEY_6,
            TABLE_SOURCE,
            TABLE_NAME,
            Delete.getName(),
            ROW_CONVERTER,
            recordData6,
            recordData6,
            GENERIC_METADATA
    );
    public static final Row record5Update = RowFactory.create(
            "8",
            RECORD_KEY_5,
            TABLE_SOURCE,
            TABLE_NAME,
            Update.getName(),
            ROW_CONVERTER,
            recordData5,
            recordData5,
            GENERIC_METADATA
    );

    // structured load records
    public static final Row structuredRecord2Load = RowFactory
            .create(
                    RECORD_KEY_2,
                    STRING_FIELD_VALUE,
                    null,
                    NUMBER_FIELD_VALUE,
                    ARRAY_FIELD_VALUE,
                    "1",
                    Load.getName()
            );
    public static final Row structuredRecord3Load = RowFactory
            .create(
                    RECORD_KEY_3,
                    STRING_FIELD_VALUE,
                    null,
                    NUMBER_FIELD_VALUE,
                    ARRAY_FIELD_VALUE,
                    "2",
                    Load.getName()
            );
    public static final Row structuredRecord1Load = RowFactory
            .create(
                    RECORD_KEY_1,
                    STRING_FIELD_VALUE,
                    null,
                    NUMBER_FIELD_VALUE,
                    ARRAY_FIELD_VALUE,
                    "3",
                    Load.getName()
            );


    // structured insert, update, and delete records
    public static final Row structuredRecord4Insertion = RowFactory
            .create(
                    RECORD_KEY_4,
                    STRING_FIELD_VALUE,
                    null,
                    NUMBER_FIELD_VALUE,
                    ARRAY_FIELD_VALUE,
                    "0",
                    Insert.getName()
            );
    public static final Row structuredRecord5Insertion = RowFactory
            .create(
                    RECORD_KEY_5,
                    STRING_FIELD_VALUE,
                    null,
                    NUMBER_FIELD_VALUE,
                    ARRAY_FIELD_VALUE,
                    "4",
                    Insert.getName()
            );
    public static final Row structuredRecord6Insertion = RowFactory
            .create(
                    RECORD_KEY_6,
                    STRING_FIELD_VALUE,
                    null,
                    NUMBER_FIELD_VALUE,
                    ARRAY_FIELD_VALUE,
                    "5",
                    Insert.getName()
            );
    public static final Row structuredRecord7Update = RowFactory
            .create(
                    RECORD_KEY_7,
                    STRING_FIELD_VALUE,
                    null,
                    NUMBER_FIELD_VALUE,
                    ARRAY_FIELD_VALUE,
                    "6",
                    Update.getName()
            );
    public static final Row structuredRecord6Deletion = RowFactory
            .create(
                    RECORD_KEY_6,
                    STRING_FIELD_VALUE,
                    null,
                    NUMBER_FIELD_VALUE,
                    ARRAY_FIELD_VALUE,
                    "7",
                    Delete.getName()
            );
    public static final Row structuredRecord5Update = RowFactory
            .create(
                    RECORD_KEY_5,
                    STRING_FIELD_VALUE,
                    null,
                    NUMBER_FIELD_VALUE,
                    ARRAY_FIELD_VALUE,
                    "8",
                    Update.getName()
            );

    public static final Row structuredRecord4SecondUpdate = RowFactory
            .create(
                    RECORD_KEY_4,
                    "second update",
                    null,
                    NUMBER_FIELD_VALUE,
                    ARRAY_FIELD_VALUE,
                    "4",
                    Update.getName()
            );

    public static Dataset<Row> createTestDataset(SparkSession spark) {
        val rawData = new ArrayList<>(Arrays.asList(
                record1Load,
                record2Load,
                record3Load,
                record4Insert,
                record5Insert,
                record6Insert,
                record7Update,
                record6Deletion,
                record5Update
        ));

        return spark.createDataFrame(rawData, ROW_SCHEMA);
    }

    public static Dataset<Row> createNonUniqueUnorderedTestDataset(SparkSession spark) {

        val record4FirstInsertion = RowFactory.create(
                "0",
                RECORD_KEY_4,
                TABLE_SOURCE,
                TABLE_NAME,
                Insert.getName(),
                ROW_CONVERTER,
                recordData4.replaceAll(STRING_FIELD_VALUE, "first insert"),
                recordData4.replaceAll(STRING_FIELD_VALUE, "first insert"),
                GENERIC_METADATA
        );

        val record4FirstUpdate = RowFactory.create(
                "1",
                RECORD_KEY_4,
                TABLE_SOURCE,
                TABLE_NAME,
                Update.getName(),
                ROW_CONVERTER,
                recordData4.replaceAll(STRING_FIELD_VALUE, "first update"),
                recordData4.replaceAll(STRING_FIELD_VALUE, "first update"),
                GENERIC_METADATA
        );

        val record4Deletion = RowFactory.create(
                "2",
                RECORD_KEY_4,
                TABLE_SOURCE,
                TABLE_NAME,
                Delete.getName(),
                ROW_CONVERTER,
                recordData4.replaceAll(STRING_FIELD_VALUE, "delete"),
                recordData4.replaceAll(STRING_FIELD_VALUE, "delete"),
                GENERIC_METADATA
        );

        val record4SecondInsertion = RowFactory.create(
                "3",
                RECORD_KEY_4,
                TABLE_SOURCE,
                TABLE_NAME,
                Insert.getName(),
                ROW_CONVERTER,
                recordData4.replaceAll(STRING_FIELD_VALUE, "second insert"),
                recordData4.replaceAll(STRING_FIELD_VALUE, "second insert"),
                GENERIC_METADATA
        );

        val record4SecondUpdate = RowFactory.create(
                "4",
                RECORD_KEY_4,
                TABLE_SOURCE,
                TABLE_NAME,
                Update.getName(),
                ROW_CONVERTER,
                recordData4.replaceAll(STRING_FIELD_VALUE, "second update"),
                recordData4.replaceAll(STRING_FIELD_VALUE, "second update"),
                GENERIC_METADATA
        );

        val rawData = Arrays.asList(
                record4FirstInsertion,
                record4FirstUpdate,
                record4Deletion,
                record4SecondInsertion,
                record4SecondUpdate,
                record6Insert
        );

        Collections.shuffle(rawData);

        return spark.createDataFrame(rawData, ROW_SCHEMA);
    }

    public static Dataset<Row> createExpectedRawDataset(SparkSession spark) {
        val expectedRawData = new ArrayList<Row>();

        expectedRawData.add(
                RowFactory.create(
                        RECORD_KEY_1 + ":3:" + Load.getName(),
                        "3",
                        RECORD_KEY_1,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Load.getName(),
                        ROW_CONVERTER,
                        recordData1
                )
        );

        expectedRawData.add(
                RowFactory.create(
                        RECORD_KEY_2 + ":1:" + Load.getName(),
                        "1",
                        RECORD_KEY_2,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Load.getName(),
                        ROW_CONVERTER,
                        recordData2
                )
        );
        expectedRawData.add(
                RowFactory.create(
                        RECORD_KEY_3 + ":2:" + Load.getName(),
                        "2",
                        RECORD_KEY_3,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Load.getName(),
                        ROW_CONVERTER,
                        recordData3
                )
        );

        expectedRawData.add(
                RowFactory.create(
                        RECORD_KEY_4 + ":0:" + Insert.getName(),
                        "0",
                        RECORD_KEY_4,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Insert.getName(),
                        ROW_CONVERTER,
                        recordData4
                )
        );

        expectedRawData.add(
                RowFactory.create(
                        RECORD_KEY_5 + ":4:" + Insert.getName(),
                        "4",
                        RECORD_KEY_5,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Insert.getName(),
                        ROW_CONVERTER,
                        recordData5
                )
        );

        expectedRawData.add(
                RowFactory.create(
                        RECORD_KEY_6 + ":5:" + Insert.getName(),
                        "5",
                        RECORD_KEY_6,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Insert.getName(),
                        ROW_CONVERTER,
                        recordData6
                )
        );

        expectedRawData.add(
                RowFactory.create(
                        RECORD_KEY_7 + ":6:" + Update.getName(),
                        "6",
                        RECORD_KEY_7,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Update.getName(),
                        ROW_CONVERTER,
                        recordData7
                )
        );

        expectedRawData.add(
                RowFactory.create(
                        RECORD_KEY_6 + ":7:" + Delete.getName(),
                        "7",
                        RECORD_KEY_6,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Delete.getName(),
                        ROW_CONVERTER,
                        recordData6
                )
        );

        expectedRawData.add(
                RowFactory.create(
                        RECORD_KEY_5 + ":8:" + Update.getName(),
                        "8",
                        RECORD_KEY_5,
                        TABLE_SOURCE,
                        TABLE_NAME,
                        Update.getName(),
                        ROW_CONVERTER,
                        recordData5
                )
        );

        return spark.createDataFrame(expectedRawData, EXPECTED_RAW_SCHEMA);
    }

    public static Dataset<Row> createStructuredLoadDataset(SparkSession spark) {
        val expectedLoadData = new ArrayList<>(Arrays.asList(
                structuredRecord1Load,
                structuredRecord2Load,
                structuredRecord3Load
        ));

        return spark.createDataFrame(expectedLoadData, STRUCTURED_RECORD_WITH_OPERATION_SCHEMA);
    }

    public static Dataset<Row> createStructuredIncrementalDataset(SparkSession spark) {
        val expectedIncrementalData = new ArrayList<>(Arrays.asList(
                structuredRecord4Insertion,
                structuredRecord7Update,
                structuredRecord6Deletion,
                structuredRecord5Update
        ));

        return spark.createDataFrame(expectedIncrementalData, STRUCTURED_RECORD_WITH_OPERATION_SCHEMA);
    }

    public static Dataset<Row> createExpectedUniqueMostRecentRecordsInWriteOrder(SparkSession spark) {
        val expectedIncrementalData = Arrays.asList(
                structuredRecord4SecondUpdate,
                structuredRecord6Insertion
        );

        return spark.createDataFrame(expectedIncrementalData, STRUCTURED_RECORD_WITH_OPERATION_SCHEMA);
    }

    public static Dataset<Row> createExpectedRecordsInWriteOrder(SparkSession spark) {
        val expectedIncrementalData = new ArrayList<>(Arrays.asList(
                structuredRecord4Insertion,
                structuredRecord7Update,
                structuredRecord6Deletion,
                structuredRecord5Update
        ));

        return spark.createDataFrame(expectedIncrementalData, STRUCTURED_RECORD_WITH_OPERATION_SCHEMA);
    }

    public static Dataset<Row> createStructuredInsertDataset(SparkSession spark) {
        val expectedIncrementalData = new ArrayList<>(Arrays.asList(
                structuredRecord4Insertion,
                structuredRecord5Insertion,
                structuredRecord6Insertion
        ));

        return spark.createDataFrame(expectedIncrementalData, STRUCTURED_RECORD_WITH_OPERATION_SCHEMA);
    }

    public static Dataset<Row> createStructuredUpdateDataset(SparkSession spark) {
        val expectedIncrementalData = new ArrayList<>(Arrays.asList(
                structuredRecord7Update,
                structuredRecord5Update
        ));

        return spark.createDataFrame(expectedIncrementalData, STRUCTURED_RECORD_WITH_OPERATION_SCHEMA);
    }

    public static Dataset<Row> createStructuredDeleteDataset(SparkSession spark) {
        val expectedIncrementalData = new ArrayList<>(Collections.singletonList(structuredRecord6Deletion));

        return spark.createDataFrame(expectedIncrementalData, STRUCTURED_RECORD_WITH_OPERATION_SCHEMA);
    }

    // Private constructor to prevent instantiation.
    private ZoneFixtures() {}
}
