package simple;

import es.bsc.compss.util.ResourceManager;

import java.io.FileInputStream;
import java.io.FileOutputStream;


public class SimpleExtAdaptation {

    private static final String counterName = "counter";

    /**
     * Polls until {@code ResourceManager.getAllWorkers().size() == expected} or the
     * deadline is reached, checking every second.  Fails fast and exits on timeout.
     *
     * @param expected  target worker count
     * @param timeoutMs maximum milliseconds to wait
     * @param label     description printed in failure/success messages
     */
    private static void waitForWorkerCount(int expected, long timeoutMs, String label)
        throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        int current;
        do {
            current = ResourceManager.getAllWorkers().size();
            if (current == expected) {
                System.out.println("** " + label + " OK (workers=" + current + ") **");
                return;
            }
            Thread.sleep(1_000);
        } while (System.currentTimeMillis() < deadline);
        // One last check after the deadline expires
        current = ResourceManager.getAllWorkers().size();
        if (current != expected) {
            System.out.println("FAIL: " + label + " timed out: expected " + expected
                + " workers, got " + current);
            System.exit(-1);
        }
        System.out.println("** " + label + " OK (workers=" + current + ") **");
    }


    public static void main(String[] args) {
        if (args.length != 5) {
            System.out.println("[ERROR] Incorrect number of parameters");
            System.out.println("    Usage simple <initVal> <increment> <minVM> <maxVM> <creationTime>");
            System.exit(-1);
        }

        FileOutputStream fos;
        FileInputStream fis;
        try {
            int initialValue = Integer.parseInt(args[0]);
            int increment = Integer.parseInt(args[1]);
            int minVM = Integer.parseInt(args[2]);
            int maxVM = Integer.parseInt(args[3]);
            int creationTime = Integer.parseInt(args[4]);
            System.out.println("[LOG] Initial counter value is " + initialValue);
            System.out.println("[LOG] Creating VM time is " + creationTime);

            // Check initial values
            int currentRes = ResourceManager.getAllWorkers().size();
            System.out.println("[LOG] Initial number of resources is " + currentRes);
            if (currentRes != 0) {
                System.out.println("FAIL: Initial Resources incorrect " + currentRes);
                System.exit(-1);
            } else {
                System.out.println("** Initial Resource detection  OK **");
            }

            // Poll until the initial minVM VMs are ready
            System.out.println("[LOG] Waiting for minimal number of VMs (" + minVM + ")...");
            waitForWorkerCount(minVM, creationTime * 2000L, "Initial VM creation");

            // Poll until the externally-added VMs reach maxVM, then wait briefly
            // for all NIO workers to finish their handshake before submitting tasks.
            System.out.println("[LOG] Waiting for externally-added VMs (" + maxVM + ")...");
            waitForWorkerCount(maxVM, creationTime * 2000L, "Extra VM creation");
            Thread.sleep(5_000);

            // Execute increment tasks
            System.out.println("[LOG] Sending increment executions");
            for (int i = 0; i < increment; i++) {
                fos = new FileOutputStream(counterName + i);
                fos.write(initialValue);
                fos.close();
                SimpleImpl.increment(counterName + i);
            }

            // Sync results and verify correctness
            for (int i = 0; i < increment; i++) {
                fis = new FileInputStream(counterName + i);
                int finalValue = fis.read();
                int expected = initialValue + 1;
                System.out.println("[LOG] Final counter" + i + " value is " + finalValue
                    + " (expected: " + expected + ")");
                fis.close();
                if (finalValue != expected) {
                    System.out.println("FAIL: Incorrect final value at counter" + i);
                    System.exit(-1);
                }
            }

            System.out.println("** Application values OK **");

            // Poll until the scheduler has scaled back to minVM after external REMOVE commands,
            // then pause briefly so applyPolicies logs currentVMs==minVM before exit.
            System.out.println("[LOG] Waiting for scale-down to " + minVM + " VMs...");
            waitForWorkerCount(minVM, creationTime * 3000L, "Intermediate VM destruction");
            Thread.sleep(5_000);

        } catch (Exception ioe) {
            System.out.println("[ERROR] Exception found");
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

}
