package remoteFile;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class Simple {
    
    private static final String counterIN = "file://COMPSsWorker01@/tmp/COMPSsWorker01/counter";
    private static final String counterPathIN = "/tmp/COMPSsWorker01/counter";
    private static final String counterPathOUT = "counter";

    public static void main(String[] args) {
        // Check and get parameters
        if (args.length != 1) {
            System.out.println("[ERROR] Bad number of parameters");
            System.out.println("    Usage: simple.Simple <counterValue>");
            System.exit(-1);
        }
        
        int initialValue = Integer.parseInt(args[0]);

        // ------------------------------------------------------------------------
        // Write value
        try {
            FileOutputStream fos = new FileOutputStream(counterPathIN);
            fos.write(initialValue);
            System.out.println("Initial counter value is " + initialValue);
            fos.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }

        // ------------------------------------------------------------------------
        // Execute increment
        SimpleImpl.increment(counterIN, counterPathOUT);

        // ------------------------------------------------------------------------
        // Read new value
        System.out.println("After Sending task");
        try {
            FileInputStream fis = new FileInputStream(counterPathOUT);
            System.out.println("Final counter value is " + fis.read());
            fis.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

}
