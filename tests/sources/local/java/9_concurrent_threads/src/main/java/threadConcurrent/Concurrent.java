package threadConcurrent;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class Concurrent extends Thread {

    private static int NUM_THREADS = 2;

    private String counterName = "counter";
    private int value;


    public Concurrent() {
        this.value = 0;
    }

    public Concurrent(int initVal) {
        this.value = initVal;
        this.counterName = "counter" + String.valueOf(this.value);
    }

    public void run() {
        // Write value
        try {
            FileOutputStream fos = new FileOutputStream(counterName);
            fos.write(value);
            System.out.println("Initial counter value is " + value);
            fos.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }

        // ------------------------------------------------------------------------
        // Execute increment
        ConcurrentImpl.increment(counterName);

        // Read new value
        try {
            FileInputStream fis = new FileInputStream(counterName);
            System.out.println("Final counter value is " + fis.read());
            fis.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

    public static void main(String[] args) {
        // Check and get parameters
        if (args.length != 1) {
            System.err.println("[ERROR] Bad number of parameters");
            System.err.println("    Usage: threadConcurrent.Concurrent <counterValue>");
            System.exit(-1);
        }
        int initialValue = Integer.parseInt(args[0]);

        // Launch main concurrent threads
        Concurrent[] threads = new Concurrent[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; ++i) {
            threads[i] = new Concurrent(initialValue);
            threads[i].start();
            initialValue = initialValue + 1;
        }

        // Active waiting
        for (int i = 0; i < NUM_THREADS; ++i) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                System.err.println("[ERROR] Interrupted thread");
                System.exit(-1);
            }
        }
    }

}
