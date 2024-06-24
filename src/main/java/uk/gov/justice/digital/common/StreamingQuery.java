package uk.gov.justice.digital.common;

import static uk.gov.justice.digital.common.ResourcePath.ensureEndsWithSlash;

public class StreamingQuery {

    private static final String CHECKPOINT_FOLDER_PATH = "DataHubCdcJob";

    private static final String QUERY_NAME_PREFIX = "Datahub_CDC";

    public static String getQueryName(String source, String table) {
        return QUERY_NAME_PREFIX + "_" + source + "." + table;
    }

    public static String getQueryCheckpointPath(String checkpointLocation, String source, String table) {
        return ensureEndsWithSlash(checkpointLocation) + CHECKPOINT_FOLDER_PATH + "/" + getQueryName(source, table);
    }

    private StreamingQuery() {}

}
