package testCOMPSs;

import compss.NESTED;
import es.bsc.compss.api.COMPSs;


public class Main {

    private static final int SLEEP_TIME = 5_000;


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
        System.out.println("[LOG] Test nested COMPSs single node");
        COMPSsNestedSingleNode();
        COMPSs.barrier();

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test nested COMPSs multiple nodes FIT");
        COMPSsNestedMultiNodeFit();
        COMPSs.barrier();

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test nested COMPSs multiple nodes NO FIT");
        COMPSsNestedMultiNodeNoFit();
        COMPSs.barrier();

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test multiple concurrent nested COMPSs");
        COMPSsConcurrentMultiNode();
        COMPSs.barrier();

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test multiple concurrent nested COMPSs");
        COMPSsNoWorkerInMasterFit();
        COMPSs.barrier();

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test multiple concurrent nested COMPSs");
        COMPSsNoWorkerInMasterNoFit();
        COMPSs.barrier();

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test nested log dir");
        COMPSsNestedLogDir();
        COMPSs.barrier();

        // ------------------------------------------------------------------------
        System.out.println("[LOG] COMPSs Test finished");
    }

    private static void usage() {
        System.err.println("ERROR: Invalid arguments");
        System.err.println("Usage: main");

        System.exit(1);
    }

    private static void COMPSsNestedSingleNode() {
        int ev = NESTED.taskSingleNode(1);
        if (ev != 0) {
            System.err.println("[ERROR] Process returned non-zero exit value: " + ev);
            System.exit(1);
        }

        int ev2 = NESTED.taskSingleNodeComplete(1);
        if (ev2 != 0) {
            System.err.println("[ERROR] Process returned non-zero exit value: " + ev2);
            System.exit(1);
        }
    }

    private static void COMPSsNestedMultiNodeFit() {
        int ev = NESTED.taskMultiNodeFit(1);

        if (ev != 0) {
            System.err.println("[ERROR] Process returned non-zero exit value: " + ev);
            System.exit(1);
        }
    }

    private static void COMPSsNestedMultiNodeNoFit() {
        int ev = NESTED.taskMultiNodeNoFit(1);

        if (ev != 0) {
            System.err.println("[ERROR] Process returned non-zero exit value: " + ev);
            System.exit(1);
        }
    }

    private static void COMPSsConcurrentMultiNode() {
        Integer ev1 = NESTED.taskConcurrentMultiNode(1);
        Integer ev2 = NESTED.taskConcurrentMultiNode(1);

        if (ev1 != 0 || ev2 != 0) {
            System.err.println("[ERROR] One process returned non-zero exit value: " + ev1 + " or " + ev2);
            System.exit(1);
        }
    }

    private static void COMPSsNoWorkerInMasterFit() {
        Integer ev = NESTED.taskNoWorkerInMasterFit(1);

        if (ev != 0) {
            System.err.println("[ERROR] Process returned non-zero exit value: " + ev);
            System.exit(1);
        }
    }

    private static void COMPSsNoWorkerInMasterNoFit() {
        Integer ev = NESTED.taskNoWorkerInMasterNoFit(1);

        if (ev != 0) {
            System.err.println("[ERROR] Process returned non-zero exit value: " + ev);
            System.exit(1);
        }
    }

    private static void COMPSsNestedLogDir() {
        Integer ev = NESTED.taskBaseLogDir(1);

        if (ev != 0) {
            System.err.println("[ERROR] Process returned non-zero exit value: " + ev);
            System.exit(1);
        }
    }

}
