package basic.tasks;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class Simple {

    public static final String COUNTER_NAME = "counter";


    /**
     * Main test function.
     * 
     * @param args System arguments.
     * @throws Exception When an error occurs.
     */
    public static void main(String[] args) throws Exception {
        // Check and get parameters
        if (args.length != 1) {
            System.out.println("[ERROR] Bad number of parameters");
            System.out.println("    Usage: simple.Simple <counterValue>");
            System.exit(-1);
        }
        int initialValue = Integer.parseInt(args[0]);

        // Write value
        try (FileOutputStream fos = new FileOutputStream(COUNTER_NAME)) {
            fos.write(initialValue);
            System.out.println("Initial counter value is " + initialValue);
        }

        // ------------------------------------------------------------------------
        // Execute increment
        SimpleImpl.increment(COUNTER_NAME);

        // Read new value
        System.out.println("After Sending task");
        String test = "Name= " + COUNTER_NAME;
        System.out.println("In method Checking result " + test);
        try (FileInputStream fis = new FileInputStream(COUNTER_NAME)) {
            System.out.println("Final counter value is " + fis.read());
        }

        // Execute increment
        SimpleImpl.increment(COUNTER_NAME);
        // Read from private method
        checkResult(COUNTER_NAME);

        // Execute increment
        SimpleImpl.increment(COUNTER_NAME);
        // Read from blackbox method
        Reader.checkResult(COUNTER_NAME);
    }

    private static void checkResult(String counterName) throws IOException {
        System.out.println("Private Checking result");
        try (FileInputStream fis = new FileInputStream(counterName)) {
            System.out.println("Final counter 2 value is " + fis.read());
        }
    }

}
