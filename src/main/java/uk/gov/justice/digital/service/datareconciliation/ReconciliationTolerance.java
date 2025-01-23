package uk.gov.justice.digital.service.datareconciliation;

import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.lang.String.format;


public class ReconciliationTolerance {

    private ReconciliationTolerance() {
    }

    /**
     * Determines if the provided values are equal with the allowed tolerances.
     * @param value1 The 1st value to compare
     * @param value2 The 2nd value to compare
     * @param absoluteTolerance The tolerance allowed, expressed as an absolute value which must be non-negative.
     *                          E.g. If the values are 1 and 3 then an absoluteTolerance of 1
     * @param relativeTolerance The tolerance allowed, relative to the greater of the two values, expressed as a
     *                          percentage between 0.0 and 1.0.
     * @return True if the values are equal within the tolerances, otherwise false.
     */
    public static boolean equalWithTolerance(long value1, long value2, long absoluteTolerance, double relativeTolerance) {
        if (value1 < 0 || value2 < 0) {
            throw new IllegalArgumentException(format("Values must be non-negative. Actual values: %s, %s", value1, value2));
        }
        if (absoluteTolerance < 0) {
            throw new IllegalArgumentException("Absolute tolerance must be non-negative");
        }
        if (relativeTolerance < 0 || relativeTolerance > 1 || Double.isNaN(relativeTolerance)) {
            throw new IllegalArgumentException("Percentage must be between 0.0 and 1.0. Actual value: " + relativeTolerance);
        }
        // Take the largest between the absolute tolerance and the relative tolerance applied to the largest value
        double allowedDifference = max(absoluteTolerance, max(value1, value2) * relativeTolerance);
        long difference = abs(value1 - value2);
        return difference <= allowedDifference;
    }
}
