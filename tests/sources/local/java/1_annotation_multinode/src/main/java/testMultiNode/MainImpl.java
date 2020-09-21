package testMultiNode;

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
        if (hostnames == null || hostnames.isEmpty() || !hostnames.equals(expectedHostnames)) {
            System.err.println("ERROR: Incorrect hostlist");
            System.err.println("  - Expected: " + expectedHostnames);
            System.err.println("  - Got: " + hostnames);
            return 1;
        }

        // All ok
        return 0;
    }

}
