package uk.gov.justice.digital.client.s3;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static uk.gov.justice.digital.client.s3.S3DataProvider.schemasMatch;

class S3DataProviderTest {

    @Test
    public void sameObjectShouldMatch() {
        StructType schema = new StructType(new StructField[]{
                new StructField("column 1", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("column 2", DataTypes.IntegerType, true, Metadata.empty()),
        });

        assertTrue(schemasMatch(schema, schema));
    }
    @Test
    public void identicalSchemasShouldMatch() {
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
    public void sameSchemasButWithDifferentNullabilityShouldMatch() {
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
    public void schemaWithExtraColumnShouldNotMatch() {
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
    public void schemaWithMissingColumnShouldNotMatch() {
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
    public void sameColumnsExceptDifferentDataTypesShouldNotMatch() {
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
    public void sameColumnsExceptDifferentMetadataShouldMatch() {
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
    public void identicalNestedStructsShouldMatch() {
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
    public void differentNestedStructColumnNamesShouldNotMatch() {
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
    public void differentNestedStructTypesShouldNotMatch() {
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
    public void differentNestedStructNullabilityShouldMatch() {
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
    public void differentNestedStructMetadataShouldMatch() {
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
    public void schemaWithShortInferredIntSpecifiedShouldMatch() {
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
    public void schemaWithIntInferredShortSpecifiedShouldNotMatch() {
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
}