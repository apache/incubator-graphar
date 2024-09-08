package org.apache.graphar.info;

import org.junit.Assume;

public class TestUtil {
    private static String GAR_TEST_DATA = null;

    public static String getTestData() {
        return GAR_TEST_DATA;
    }

    public static void checkTestData() {
        if (GAR_TEST_DATA == null) {
            GAR_TEST_DATA = System.getenv("GAR_TEST_DATA");
        }
        Assume.assumeTrue("GAR_TEST_DATA is not set", GAR_TEST_DATA != null);
    }
}
