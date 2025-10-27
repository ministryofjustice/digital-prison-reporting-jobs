package uk.gov.justice.digital.common;

import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static uk.gov.justice.digital.common.ResourcePath.tablePath;

class ResourcePathTest {

    private static final String expectedRelativePath = "foo/bar/baz";
    private static final String expectedS3Path = "s3://foo/bar/baz";

    @Test
    void shouldReturnAUriStringGivenValidRelativePathElements() {
        String[] elements = { "foo", "bar", "baz" };
        val result = ResourcePath.createValidatedPath(elements);
        assertEquals(expectedRelativePath, result);
    }

    @Test
    void shouldReturnAUriStringGivenValidS3PathElements() {
        String[] elements = { "s3://foo", "bar", "baz" };
        val result = ResourcePath.createValidatedPath(elements);
        assertEquals(expectedS3Path, result);
    }

    @Test
    void shouldReturnAUriStringGivenValidRelativePathElementsWithRedundantSeparators() {
        String[] elements = { "foo", "bar/", "///baz" };
        val result = ResourcePath.createValidatedPath(elements);
        assertEquals(expectedRelativePath, result);
    }

    @Test
    void shouldReturnAUriStringGivenValidS3PathElementsWithRedundantSeparators() {
        String[] elements = { "s3://foo////", "bar/", "///baz" };
        val result = ResourcePath.createValidatedPath(elements);
        assertEquals(expectedS3Path, result);
    }

    @Test
    void shouldThrowIllegalArgumentExceptionIfPathCannotBeValidated() {
        String[] elements = { "\foo", "\nbar" };
        assertThrows(IllegalArgumentException.class, () -> ResourcePath.createValidatedPath(elements));
    }

    @Test
    void tablePathShouldAddSlashWhenHasNoSlash() {
        String result = tablePath("s3://root", "source", "table");
        assertEquals("s3://root/source/table", result);
    }

    @Test
    void tablePathRootShouldNotAddSlashWhenHasSlash() {
        String result = tablePath("s3://root/", "source", "table");
        assertEquals("s3://root/source/table", result);
    }

}
