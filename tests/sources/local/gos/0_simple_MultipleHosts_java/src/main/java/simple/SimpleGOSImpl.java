package simple;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;


public class SimpleGOSImpl {

    /**
     * Increment cpu needed.
     */
    public static void incrementCPU(String counterFile) {
        try {
            FileInputStream fis = new FileInputStream(counterFile);
            int count = fis.read();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Increment cpu needed.
     */
    public static void incrementGPU(String counterFile) {
        try {
            FileInputStream fis = new FileInputStream(counterFile);
            int count = fis.read();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

}
