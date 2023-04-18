package uk.gov.justice.digital.common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;

public class Util {

    protected void handleError(final Exception e) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        System.err.print(sw.getBuffer().toString());
    }

    protected boolean schemaContains(final Dataset<Row> dataFrame, final String field) {
        return Arrays.asList(dataFrame.schema().fieldNames()).contains(field);
    }
}
