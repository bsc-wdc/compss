package java_api_calls;

import es.bsc.compss.api.COMPSs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


/**
 * Two tasks without dependencies that are scheduled one after the other because of the barrier call
 * 
 */
public class Main {

    public static void main(String[] args) {
        // Check and get parameters
        if (args.length != 1) {
            System.out.println("[ERROR] Bad number of parameters");
            System.out.println("    Usage: simple.Simple <counterValue>");
            System.exit(-1);
        }
        String counterName1 = "counter1";
        String counterName2 = "counter2";
        int initialValue = Integer.parseInt(args[0]);

        // ------------------------------------------------------------------------
        // Write value
        try {
            FileOutputStream fos = new FileOutputStream(counterName1);
            fos.write(initialValue);
            System.out.println("Initial counter1 value is " + initialValue);
            fos.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
        // Write value
        try {
            FileOutputStream fos = new FileOutputStream(counterName2);
            fos.write(initialValue);
            System.out.println("Initial counter2 value is " + initialValue);
            fos.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }

        // ------------------------------------------------------------------------
        // Execute increment 1
        MainImpl.increment(counterName1);

        // ------------------------------------------------------------------------
        // API Call to wait for all tasks
        COMPSs.barrier();

        // ------------------------------------------------------------------------
        // Execute increment 2
        MainImpl.increment(counterName2);

        // ------------------------------------------------------------------------
        // Read new value
        System.out.println("After Sending task");
        try {
            FileInputStream fis = new FileInputStream(counterName1);
            System.out.println("Final counter1 value is " + fis.read());
            fis.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
        // Read new value
        System.out.println("After Sending task");
        try {
            FileInputStream fis = new FileInputStream(counterName2);
            System.out.println("Final counter2 value is " + fis.read());
            fis.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

}
