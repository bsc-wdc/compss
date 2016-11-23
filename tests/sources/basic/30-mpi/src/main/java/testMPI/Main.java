package testMPI;

import integratedtoolkit.api.COMPSs;
import mpi.MPI;


public class Main {

    private static final int SLEEP_TIME = 5_000;

    private static final int N = 10;
    private static final int[] data = new int[N];
    private static int totalSum;


    public static void main(String[] args) {
        // ------------------------------------------------------------------------
        System.out.println("[LOG] Check args");
        if (args.length != 0) {
            usage();
        }

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Initialize data");
        initializeData();

        // ------------------------------------------------------------------------
        // Wait for workers to load to ensure both of them are available during the test
        System.out.println("[LOG] Wait workers to initialize");
        try {
            Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e) {
            // No need to handle such exception
        }

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test MPI with 1 node");
        testMPISingleNode();
        
        // ------------------------------------------------------------------------
        System.out.println("[LOG] Wait for single node execution");
        COMPSs.waitForAllTasks();

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test MPI with 2 node");
        testMPIMultipleNodes();

        // ------------------------------------------------------------------------
        System.out.println("[LOG] MPI Test finished");
    }

    private static void usage() {
        System.err.println("ERROR: Invalid arguments");
        System.err.println("Usage: main");

        System.exit(1);
    }

    private static void initializeData() {
        totalSum = 0;
        for (int i = 0; i < N; ++i) {
            data[i] = (int) (Math.random() * 1_000);
            totalSum += data[i];
            System.out.println("Data:" + i + " Value: " + data[i]);
        }
        System.out.println("TotalSum:" + totalSum);
    }

    private static void testMPISingleNode() {
        String output = MPI.taskSingleMPI(data);
        
        String[] lines = output.split("\n");
        int sum = Integer.valueOf(lines[lines.length - 1]);

        if (sum == totalSum) {
            System.out.println("[MPI_SINGLE] Received value from task is correct");
        } else {
            System.out.println("[MPI_SINGLE] Received value from task is not correct: " + sum + " vs " + totalSum);
        }
    }

    private static void testMPIMultipleNodes() {
        String output = MPI.taskMultipleMPI(data);
        
        String[] lines = output.split("\n");
        int sum = Integer.valueOf(lines[lines.length - 1]);

        if (sum == totalSum) {
            System.out.println("[MPI_MULTIPLE] Received value from task is correct");
        } else {
            System.out.println("[MPI_MULTIPLE] Received value from task is not correct: " + sum + " vs " + totalSum);
        }
    }

}
