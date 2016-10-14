package nonNativeTasks;

import binary.BINARY;
import mpi.MPI;
import ompss.OMPSS;


public class Main {

    private static final int WAIT_RUNTIME = 5_000;


    public static void main(String[] args) {

        // ----------------------------------------------------------------------------
        // Wait for Runtime to have the worker available
        System.out.println("[LOG] Wait for Runtime to be ready");
        try {
            Thread.sleep(WAIT_RUNTIME);
        } catch (InterruptedException e) {
            // No need to handle such exception
        }

        // ----------------------------------------------------------------------------
        // Launch a Normal Method Task task
        System.out.println("[LOG] Launch Normal Method task");
        testNormalTask();

        // ----------------------------------------------------------------------------
        // Launch a MPI task
        System.out.println("[LOG] Launch MPI task");
        testMPI();

        // ----------------------------------------------------------------------------
        // Launch a OMPSS task
        System.out.println("[LOG] Launch OMPSS task");
        testOMPSS();

        // ----------------------------------------------------------------------------
        // Launch a OPENCL task
        // TODO: Support OpenCL in COMPSs Runtime
        // System.out.println("[LOG] Launch OPENCL task");
        // testOPENCL();

        // ----------------------------------------------------------------------------
        // Launch a BINARY task
        System.out.println("[LOG] Launch BINARY task");
        testBINARY();
    }

    private static void testNormalTask() {
        // Task call
        int result = MainImpl.normalTask("Hello World!");

        // Synchronize result
        System.out.println("[RESULT] Normal Task: " + result);
    }

    private static void testMPI() {
        // Task call
        String output = MPI.mpiTask("Hello World!");

        // Synchronize result
        for (String line : output.split("\n")) {
            System.out.println("[RESULT] MPI Task: " + line);
        }
    }

    private static void testOMPSS() {
        // Task call
        String output = OMPSS.ompssTask("Hello World!");

        // Synchronize result
        for (String line : output.split("\n")) {
            System.out.println("[RESULT] MPI Task: " + line);
        }
    }

    /*
     * private static void testOPENCL() { // Task call String output = OPENCL.openclTask("Hello World!");
     * 
     * // Synchronize result for (String line : output.split("\n")) { System.out.println("[RESULT] MPI Task: " + line);
     * } }
     */

    private static void testBINARY() {
        // Task call
        String output = BINARY.binaryTask("Hello World!");

        // Synchronize result
        for (String line : output.split("\n")) {
            System.out.println("[RESULT] MPI Task: " + line);
        }
    }

}
