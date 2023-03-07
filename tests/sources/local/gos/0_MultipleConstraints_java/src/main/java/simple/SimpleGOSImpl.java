package simple;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;


public class SimpleGOSImpl {

    /**
     * Task cpu.
     */
    public static void taskCPU(String counterFile) {
        try {
            FileInputStream fis = new FileInputStream(counterFile);
            int count = fis.read();
            System.out.println("[[[[[[[[[[[[[[[[[[[" + count + "]]]]]]]]]]]]]]]]]]]");
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Task gpu.
     */
    public static void taskGPU(String counterFile) {
        try {
            FileInputStream fis = new FileInputStream(counterFile);
            int count = fis.read();
            System.out.println("[[[[[[[[[[[[[[[[[[[" + count + "]]]]]]]]]]]]]]]]]]]");
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Task software.
     */
    public static void taskSoftware(String counterFile) {
        try {
            FileInputStream fis = new FileInputStream(counterFile);
            int count = fis.read();
            System.out.println("[[[[[[[[[[[[[[[[[[[" + count + "]]]]]]]]]]]]]]]]]]]");
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

}
