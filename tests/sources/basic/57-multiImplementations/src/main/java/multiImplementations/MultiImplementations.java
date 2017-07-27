package multiImplementations;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import es.bsc.compss.api.COMPSs;

import binary.BINARY;
import mpi.MPI;


public class MultiImplementations {

    private static final int SLEEP_WAIT_FOR_RUNTIME = 4_000; // ms
    private static final int NUM_TESTS = 5;
    private static final String FILE_NAME = "counterFile_";


    public static void main(String[] args) {
        // Check and get parameters
        if (args.length != 2) {
            System.out.println("[ERROR] Bad number of parameters");
            System.out.println("    Usage: multiImplementations.MultiImplementations <taskWidth> <counterValue>");
            System.exit(-1);
        }
        int taskWidth = Integer.parseInt(args[0]);
        int initialValue = Integer.parseInt(args[1]);

        // ------------------------------------------------------------------------
        // Initial sleep
        try {
            Thread.sleep(SLEEP_WAIT_FOR_RUNTIME);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // ------------------------------------------------------------------------
        // Initialize files
        for (int i = 0; i < NUM_TESTS * taskWidth; ++i) {
            String counterName = FILE_NAME + i;
            System.out.println("[INFO] Initializing file " + counterName + " with value " + initialValue);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(counterName))) {
                writer.write(String.valueOf(initialValue));
                writer.flush();
            } catch (IOException ioe) {
                ioe.printStackTrace();
                System.exit(-1);
            }
        }

        // ------------------------------------------------------------------------
        // Execute Method-Method tasks
        System.out.println("[INFO] Executing METHOD-METHOD versioning");
        for (int i = 0; i < taskWidth; ++i) {
            String counterName = FILE_NAME + i;
            Implementation1.methodMethod(counterName);
        }

        COMPSs.barrier();

        // ------------------------------------------------------------------------
        // Execute Method-Binary tasks
        System.out.println("[INFO] Executing METHOD-BINARY versioning");
        for (int i = taskWidth; i < 2 * taskWidth; ++i) {
            String counterName = FILE_NAME + i;
            Implementation1.methodBinary(counterName);
        }

        COMPSs.barrier();

        // ------------------------------------------------------------------------
        // Execute Binary-Method tasks
        System.out.println("[INFO] Executing BINARY-METHOD versioning");
        for (int i = 2 * taskWidth; i < 3 * taskWidth; ++i) {
            String counterName = FILE_NAME + i;
            BINARY.binaryMethod(counterName);
        }

        COMPSs.barrier();

        // ------------------------------------------------------------------------
        // Execute Method-MPI tasks
        System.out.println("[INFO] Executing METHOD-MPI versioning");
        for (int i = 3 * taskWidth; i < 4 * taskWidth; ++i) {
            String counterName = FILE_NAME + i;
            Implementation1.methodMpi(counterName);
        }

        COMPSs.barrier();

        // ------------------------------------------------------------------------
        // Execute MPI-Method tasks
        System.out.println("[INFO] Executing MPI-METHOD versioning");
        for (int i = 4 * taskWidth; i < 5 * taskWidth; ++i) {
            String counterName = FILE_NAME + i;
            MPI.mpiMethod(counterName);
        }

        COMPSs.barrier();

        // ------------------------------------------------------------------------
        // Synchronize and read final value
        System.out.println("[INFO] Synchronizing values");
        for (int i = 0; i < NUM_TESTS * taskWidth; ++i) {
            String counterName = FILE_NAME + i;
            try (BufferedReader reader = new BufferedReader(new FileReader(counterName))) {
                String strVal = reader.readLine();
                Integer count = Integer.valueOf(strVal);
                System.out.println("[INFO] Final counter value on file " + counterName + " is " + count);
            } catch (IOException ioe) {
                ioe.printStackTrace();
                System.exit(-1);
            }
        }
    }

}
