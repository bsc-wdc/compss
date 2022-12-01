package testMultiNode;

import java.util.HashMap;


public class MainImpl {

    private static final String OMP_NUM_THREADS = "OMP_NUM_THREADS";
    private static final String COMPSS_HOSTNAMES = "COMPSS_HOSTNAMES";
    private static final String COMPSS_NUM_NODES = "COMPSS_NUM_NODES";
    private static final String COMPSS_NUM_THREADS = "COMPSS_NUM_THREADS";


    public static int multiNodeTask() {
        // Define expected values
        Integer expectedNumNodes = 2;
        Integer expectedNumThreads = 2;
        String expectedHostnames = "COMPSsWorker01,COMPSsWorker01,COMPSsWorker02,COMPSsWorker02";

        // Check the environment variables
        Integer numNodes = Integer.valueOf(System.getProperty(COMPSS_NUM_NODES));
        if (numNodes != expectedNumNodes) {
            System.err.println("ERROR: Incorrect number of nodes");
            System.err.println("  - Expected: " + expectedNumNodes);
            System.err.println("  - Got: " + numNodes);
            return 1;
        }

        Integer numThreads = Integer.valueOf(System.getProperty(COMPSS_NUM_THREADS));
        if (numThreads != expectedNumThreads) {
            System.err.println("ERROR: Incorrect number of threads");
            System.err.println("  - Expected: " + expectedNumThreads);
            System.err.println("  - Got: " + numThreads);
            return 1;
        }

        Integer OMPnumThreads = Integer.valueOf(System.getProperty(OMP_NUM_THREADS));
        if (OMPnumThreads != expectedNumThreads) {
            System.err.println("ERROR: Incorrect number of OMP threads");
            System.err.println("  - Expected: " + expectedNumThreads);
            System.err.println("  - Got: " + OMPnumThreads);
            return 1;
        }

        String hostnames = System.getProperty(COMPSS_HOSTNAMES);
        if (hostnames == null || hostnames.isEmpty()) {
            System.err.println("ERROR: Incorrect hostlist");
            System.err.println("  - No hosts obtained");
            return 1;
        }

        String[] hosts = hostnames.split(",");
        String[] expectedHosts = expectedHostnames.split(",");
        if (hosts.length != expectedHosts.length) {
            System.err.println("ERROR: Incorrect hostlist");
            System.err.println("  - Expected: " + expectedHosts.length + " hosts");
            System.err.println("  - Got: " + hosts.length + " hosts");
            return 1;
        }

        HashMap<String, Integer> expectedCounts = new HashMap<>();
        HashMap<String, Integer> counts = new HashMap<>();
        for (String host : hosts) {
            Integer count = counts.get(host);
            if (count == null) {
                count = 0;
            }
            count++;
            counts.put(host, count);
        }
        for (String host : expectedHosts) {
            Integer count = expectedCounts.get(host);
            if (count == null) {
                count = 0;
            }
            count++;
            expectedCounts.put(host, count);
        }

        for (java.util.Map.Entry<String, Integer> pair : expectedCounts.entrySet()) {
            String host = pair.getKey();
            Integer expectedCount = pair.getValue();
            Integer count = counts.remove(host);
            if (expectedCount != count) {
                System.err.println("ERROR: Incorrect hostlist");
                System.err.println("  - Expected: " + expectedCount + " occurrences of host " + host);
                System.err.println("  - Got: " + count + " occurrences");
            }
        }

        if (!counts.isEmpty()) {
            System.err.println("ERROR: Incorrect hostlist");
            for (java.util.Map.Entry<String, Integer> pair : counts.entrySet()) {
                System.err.println("  - Expecting no occurrences of host " + pair.getKey());
                System.err.println("  - Got " + pair.getValue() + " occurrences");
            }
        }
        // All ok
        return 0;
    }

}
