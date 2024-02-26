package uk.gov.justice.digital.service;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.justice.digital.service.ValidationService.schemasMatch;

class ValidationServiceSchemasMatchTest {
    @Test
    void shouldMatchSameObject() {
        StructType schema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, true, Metadata.empty()),
        });

        assertTrue(schemasMatch(schema, schema));
    }
    @Test
    void shouldMatchIdenticalSchemas() {
        StructType inferredSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, true, Metadata.empty()),
        });

        StructType specifiedSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, true, Metadata.empty()),
        });

        assertTrue(schemasMatch(inferredSchema, specifiedSchema));
    }

    @Test
    void shouldMatchSameSchemasButWithDifferentNullability() {
        StructType inferredSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, true, Metadata.empty()),
        });

        StructType specifiedSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, false, Metadata.empty()),
        });

        assertTrue(schemasMatch(inferredSchema, specifiedSchema));
    }

    @Test
    void ShouldNotMatchSchemaWithExtraColumn() {
        StructType inferredSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, false, Metadata.empty()),
        });

        StructType specifiedSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("column 3", DataTypes.IntegerType, false, Metadata.empty()),
        });

        assertFalse(schemasMatch(inferredSchema, specifiedSchema));
    }

    @Test
    void shouldNotMatchSchemaWithMissingColumn() {
        StructType inferredSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("column 3", DataTypes.IntegerType, false, Metadata.empty()),
        });

        StructType specifiedSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, false, Metadata.empty()),
        });

        assertFalse(schemasMatch(inferredSchema, specifiedSchema));
    }

    @Test
    void shouldNotMatchSameColumnsExceptDifferentDataTypes() {
        StructType inferredSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.StringType, false, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, true, Metadata.empty()),
        });

        StructType specifiedSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, true, Metadata.empty()),
        });

        assertFalse(schemasMatch(inferredSchema, specifiedSchema));
    }

    @Test
    void shouldMatchSameColumnsExceptDifferentMetadata() {
        StructType inferredSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.StringType, false, Metadata.fromJson("{ \"x\": \"y\"}")),
                new StructField("column 2", DataTypes.StringType, true, Metadata.empty()),
        });

        StructType specifiedSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.StringType, false, Metadata.empty()),
                new StructField("column 2", DataTypes.StringType, true, Metadata.empty()),
        });

        assertTrue(schemasMatch(inferredSchema, specifiedSchema));
    }

    @Test
    void shouldMatchIdenticalNestedStructs() {
        StructType inferredSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", new StructType(new StructField[]{
                        new StructField("column 2-1", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("column 2-2", new StructType(new StructField[]{
                                new StructField("column 2-2-1", DataTypes.IntegerType, true, Metadata.empty()),
                                new StructField("column 2-2-2", DataTypes.IntegerType, true, Metadata.empty()),
                        }), true, Metadata.empty())
                }), true, Metadata.empty()),
        });

        StructType specifiedSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", new StructType(new StructField[]{
                        new StructField("column 2-1", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("column 2-2", new StructType(new StructField[]{
                                new StructField("column 2-2-1", DataTypes.IntegerType, true, Metadata.empty()),
                                new StructField("column 2-2-2", DataTypes.IntegerType, true, Metadata.empty()),
                        }), true, Metadata.empty())
                }), true, Metadata.empty()),
        });

        assertTrue(schemasMatch(inferredSchema, specifiedSchema));
    }

    @Test
    void shouldNotMatchDifferentNestedStructColumnNames() {
        StructType inferredSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", new StructType(new StructField[]{
                        new StructField("column 2-1", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("column 2-2", new StructType(new StructField[]{
                                new StructField("column 2-2-1", DataTypes.IntegerType, true, Metadata.empty()),
                                new StructField("column 2-2-2", DataTypes.IntegerType, true, Metadata.empty()),
                        }), true, Metadata.empty())
                }), true, Metadata.empty()),
        });

        StructType specifiedSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", new StructType(new StructField[]{
                        new StructField("column 2-1", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("column 2-2", new StructType(new StructField[]{
                                new StructField("column 2-2-1", DataTypes.IntegerType, true, Metadata.empty()),
                                new StructField("column 2-2-2-different-name", DataTypes.IntegerType, true, Metadata.empty()),
                        }), true, Metadata.empty())
                }), true, Metadata.empty()),
        });

        assertFalse(schemasMatch(inferredSchema, specifiedSchema));
    }

    @Test
    void shouldNotMatchDifferentNestedStructTypes() {
        StructType inferredSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", new StructType(new StructField[]{
                        new StructField("column 2-1", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("column 2-2", new StructType(new StructField[]{
                                new StructField("column 2-2-1", DataTypes.IntegerType, true, Metadata.empty()),
                                new StructField("column 2-2-2", DataTypes.IntegerType, true, Metadata.empty()),
                        }), true, Metadata.empty())
                }), true, Metadata.empty()),
        });

        StructType specifiedSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", new StructType(new StructField[]{
                        new StructField("column 2-1", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("column 2-2", new StructType(new StructField[]{
                                new StructField("column 2-2-1", DataTypes.IntegerType, true, Metadata.empty()),
                                new StructField("column 2-2-2", DataTypes.StringType, true, Metadata.empty()),
                        }), true, Metadata.empty())
                }), true, Metadata.empty()),
        });

        assertFalse(schemasMatch(inferredSchema, specifiedSchema));
    }

    @Test
    void shouldMatchDifferentNestedStructNullability() {
        StructType inferredSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", new StructType(new StructField[]{
                        new StructField("column 2-1", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("column 2-2", new StructType(new StructField[]{
                                new StructField("column 2-2-1", DataTypes.IntegerType, true, Metadata.empty()),
                                new StructField("column 2-2-2", DataTypes.IntegerType, true, Metadata.empty()),
                        }), true, Metadata.empty())
                }), true, Metadata.empty()),
        });

        StructType specifiedSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", new StructType(new StructField[]{
                        new StructField("column 2-1", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("column 2-2", new StructType(new StructField[]{
                                new StructField("column 2-2-1", DataTypes.IntegerType, true, Metadata.empty()),
                                new StructField("column 2-2-2", DataTypes.IntegerType, false, Metadata.empty()),
                        }), true, Metadata.empty())
                }), true, Metadata.empty()),
        });

        assertTrue(schemasMatch(inferredSchema, specifiedSchema));
    }

    @Test
    void shouldMatchDifferentNestedStructMetadata() {
        StructType inferredSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", new StructType(new StructField[]{
                        new StructField("column 2-1", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("column 2-2", new StructType(new StructField[]{
                                new StructField("column 2-2-1", DataTypes.IntegerType, true, Metadata.empty()),
                                new StructField("column 2-2-2", DataTypes.IntegerType, true, Metadata.empty()),
                        }), true, Metadata.empty())
                }), true, Metadata.empty()),
        });

        StructType specifiedSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", new StructType(new StructField[]{
                        new StructField("column 2-1", DataTypes.IntegerType, true, Metadata.empty()),
                        new StructField("column 2-2", new StructType(new StructField[]{
                                new StructField("column 2-2-1", DataTypes.IntegerType, true, Metadata.fromJson("{ \"x\": \"y\"}")),
                                new StructField("column 2-2-2", DataTypes.IntegerType, true, Metadata.empty()),
                        }), true, Metadata.empty())
                }), true, Metadata.empty()),
        });

        assertTrue(schemasMatch(inferredSchema, specifiedSchema));
    }

    @Test
    void shouldMatchSchemaWithShortInferredIntSpecified() {
        StructType inferredSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.ShortType, true, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, false, Metadata.empty()),
        });

        StructType specifiedSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, false, Metadata.empty()),
        });

        assertTrue(schemasMatch(inferredSchema, specifiedSchema));
    }

    @Test
    void shouldNotMatchSchemaWithIntInferredShortSpecified() {
        StructType inferredSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, false, Metadata.empty()),
        });

        StructType specifiedSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.ShortType, true, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, false, Metadata.empty()),
        });

        assertFalse(schemasMatch(inferredSchema, specifiedSchema));
    }

    @Test
    void shouldMatchSchemaWithByteInferredIntSpecified() {
        StructType inferredSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.ByteType, true, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, false, Metadata.empty()),
        });

        StructType specifiedSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, false, Metadata.empty()),
        });

        assertTrue(schemasMatch(inferredSchema, specifiedSchema));
    }

    @Test
    void shouldNotMatchSchemaWithIntInferredByteSpecified() {
        StructType inferredSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, false, Metadata.empty()),
        });

        StructType specifiedSchema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.ByteType, true, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, false, Metadata.empty()),
        });

        assertFalse(schemasMatch(inferredSchema, specifiedSchema));
    }

}
