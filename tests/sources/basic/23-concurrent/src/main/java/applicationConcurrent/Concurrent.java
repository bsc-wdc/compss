package applicationConcurrent;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class Concurrent {

    public static void main(String[] args) {
        // Check and get parameters
        if (args.length != 1) {
            System.out.println("[ERROR] Bad number of parameters");
            System.out.println("    Usage: applicationConcurrent.Concurrent <counterValue>");
            System.exit(-1);
        }
        int initialValue = Integer.parseInt(args[0]);
        String counterName = "counter" + String.valueOf(initialValue);

        // Write value
        try {
            FileOutputStream fos = new FileOutputStream(counterName);
            fos.write(initialValue);
            System.out.println("Initial counter value is " + initialValue);
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

}
