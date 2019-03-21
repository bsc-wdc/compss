package basicTasks;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class Simple {

    public static String COUNTER_NAME = "counter";


    public static void main(String[] args) {
        // Check and get parameters
        if (args.length != 1) {
            System.out.println("[ERROR] Bad number of parameters");
            System.out.println("    Usage: simple.Simple <counterValue>");
            System.exit(-1);
        }
        int initialValue = Integer.parseInt(args[0]);

        // Write value
        try {
            FileOutputStream fos = new FileOutputStream(COUNTER_NAME);
            fos.write(initialValue);
            System.out.println("Initial counter value is " + initialValue);
            fos.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }

        // ------------------------------------------------------------------------
        // Execute increment
        try {
            SimpleImpl.increment(COUNTER_NAME);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(-1);
        }
        // Read new value
        System.out.println("After Sending task");
        String test = "Name= " + COUNTER_NAME;
        System.out.println("In method Checking result " + test);
        try {
            FileInputStream fis = new FileInputStream(COUNTER_NAME);
            System.out.println("Final counter value is " + fis.read());
            fis.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }

        try {
            // Execute increment
            SimpleImpl.increment(COUNTER_NAME);

            // Read from private method
            checkResult(COUNTER_NAME);

            // Execute increment
            SimpleImpl.increment(COUNTER_NAME);
            // Read from blackbox method
            Reader.checkResult(COUNTER_NAME);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void checkResult(String counterName) {
        System.out.println("Private Checking result");
        try {
            FileInputStream fis = new FileInputStream(counterName);
            System.out.println("Final counter 2 value is " + fis.read());
            fis.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

}
