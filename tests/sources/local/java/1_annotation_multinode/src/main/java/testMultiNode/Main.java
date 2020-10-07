package testMultiNode;

import es.bsc.compss.api.COMPSs;


public class Main {

    private static final int SLEEP_TIME = 10_000;


    public static void main(String[] args) {
        // ------------------------------------------------------------------------
        System.out.println("[LOG] Check args");
        if (args.length != 0) {
            usage();
        }

        // ------------------------------------------------------------------------
        // Wait for workers to load to ensure both of them are available during the test
        System.out.println("[LOG] Wait workers to initialize");
        try {
            Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e) {
            // No need to handle such exception
        }

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test multinode");
        multiNodeTest();

        // ------------------------------------------------------------------------
        COMPSs.barrier();
        System.out.println("[LOG] COMPSs Test finished");
    }

    private static void usage() {
        System.err.println("ERROR: Invalid arguments");
        System.err.println("Usage: main");

        System.exit(1);
    }

    private static void multiNodeTest() {
        int ev = MainImpl.multiNodeTask();
        if (ev != 0) {
            System.err.println("[ERROR] Process returned non-zero exit value: " + ev);
            System.exit(1);
        }
    }

}
