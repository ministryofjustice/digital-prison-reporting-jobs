package uk.gov.justice.digital.common;

import java.util.regex.Pattern;

import static uk.gov.justice.digital.config.JobArguments.DEFAULT_FILE_NAME_REGEX;

public class RegexPatterns {
    public static final Pattern matchAllFiles = Pattern.compile(DEFAULT_FILE_NAME_REGEX);

    public static final Pattern parquetFileRegex = Pattern.compile("(?i).+.parquet$");

    public static final Pattern jsonOrParquetFileRegex = Pattern.compile("(?i).+.json$|.+.parquet$");

    // Extracts the ordinal and optional compact extension from the checkpoint file name. Example checkpoint file names: 1 or 1.compact
    public static final Pattern checkpointFileRegexPattern = Pattern.compile("^.*\\/(\\d+)(.compact)?$");

    // Extracts the committed file path from a line in the checkpoint file
    public static final Pattern committedFileRegexPattern = Pattern.compile("^\\{\"path\":\"s3:\\/\\/([a-zA-Z-]+)\\/(.*)\",.*");

    // Extracts the bucket and the folder path from the checkpoint location
    // As an example s3://dpr-glue-jobs-development/checkpoint/dpr-reporting-hub-cdc-establishments-development/
    // is extracted to:
    // bucket - dpr-glue-jobs-development
    // folder path - checkpoint/dpr-reporting-hub-cdc-establishments-development/
    public static final Pattern checkpointRegexPattern = Pattern.compile("^s3[A-Za-z]?:\\/\\/([A-Za-z-]+)\\/([\\S\\s]+\\S+)");

    public static final String TABLE_NAME_PATTERN = "^[a-z_0-9]*$";

    public static final Pattern tableNameRegex = Pattern.compile(TABLE_NAME_PATTERN);

    // Private constructor to prevent instantiation.
    private RegexPatterns() { }
}
