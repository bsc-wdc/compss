package api.calls;

import es.bsc.compss.api.COMPSs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


/**
 * Three tasks without dependencies that are scheduled one after the other because of the barrier call.
 */
public class Main {

    /*
     * HELPER METHODS
     */

    private static void initCounter(String counterName, int value) throws IOException {
        // Write value
        try (FileOutputStream fos = new FileOutputStream(counterName)) {
            fos.write(value);
            System.out.println("Initial " + counterName + " value is " + value);
        }
    }

    private static void printCounter(String counterName) throws IOException {
        System.out.println("After Sending task");
        try (FileInputStream fis = new FileInputStream(counterName)) {
            System.out.println("Final " + counterName + " value is " + fis.read());
        }
    }

    private static void testBarrier(int initialValue) throws IOException {
        // Initialize independent counters
        String counterName1 = "counter1";
        String counterName2 = "counter2";
        String counterName3 = "counter3";
        initCounter(counterName1, initialValue);
        initCounter(counterName2, initialValue);
        initCounter(counterName3, initialValue);

        // Execute task
        MainImpl.increment(counterName1);
        // Regular barrier
        COMPSs.barrier();

        // Execute task
        MainImpl.increment(counterName2);
        // Barrier with noMoreTasks false
        COMPSs.barrier(false);

        MainImpl.increment(counterName3);
        // Barrier with noMoreTasks true
        COMPSs.barrier(true);

        // Retrieve counter results
        printCounter(counterName1);
        printCounter(counterName2);
        printCounter(counterName3);
    }

    /**
     * Test main method.
     * 
     * @param args System arguments
     * @throws IOException When processing test files.
     */
    public static void main(String[] args) throws IOException {
        // Check and get parameters
        if (args.length != 1) {
            System.out.println("[ERROR] Bad number of parameters");
            System.out.println("    Usage: java_api_calls.Main <counterValue>");
            System.exit(-1);
        }
        int initialValue = Integer.parseInt(args[0]);

        // ------------------------------------------------------------------------
        // Barrier test
        testBarrier(initialValue);
    }

}
