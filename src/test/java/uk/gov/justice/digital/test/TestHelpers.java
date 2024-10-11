package uk.gov.justice.digital.test;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.NotNull;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import uk.gov.justice.digital.config.JobArguments;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.when;

public class TestHelpers {

    public static <T> Seq<T> convertListToSeq(List<T> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    @NotNull
    public static <T> Matcher<Iterable<? extends T>> containsTheSameElementsInOrderAs(List<T> expectedItems) {
        return contains(expectedItems.stream().map(Matchers::equalTo).collect(Collectors.toList()));
    }

    public static void givenConfiguredRetriesJobArgs(int numRetries, JobArguments mockJobArguments) {
        when(mockJobArguments.getDataStorageRetryMaxAttempts()).thenReturn(numRetries);
        when(mockJobArguments.getDataStorageRetryMinWaitMillis()).thenReturn(1L);
        when(mockJobArguments.getDataStorageRetryMaxWaitMillis()).thenReturn(10L);
        when(mockJobArguments.getDataStorageRetryJitterFactor()).thenReturn(0.1D);
    }

    private TestHelpers() { }

}
