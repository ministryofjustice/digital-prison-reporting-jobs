package uk.gov.justice.digital.common;

import org.jetbrains.annotations.NotNull;

public class CheckpointFile implements Comparable<CheckpointFile> {

    private final Long fileId;
    private final boolean compact;
    private final String s3key;

    public CheckpointFile(Long fileId, boolean compact, String s3key) {
        this.fileId = fileId;
        this.compact = compact;
        this.s3key = s3key;
    }

    public Long getfileId() {
        return fileId;
    }

    public boolean isCompact() {
        return compact;
    }

    public String getS3key() {return s3key;}

    @Override
    public int compareTo(@NotNull CheckpointFile other) {
        return this.getfileId().compareTo(other.getfileId());
    }
}
