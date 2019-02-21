package deleteFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class Simple {

    public static void main(String[] args) {
        // Check and get parameters
        if (args.length != 1) {
            System.out.println("[ERROR] Bad number of parameters");
            System.out.println("    Usage: simple.Simple <counterValue>");
            System.exit(-1);
        }
        String counterName = "counter_INOUT";
        String counterNameIN = "counter_IN";
        String counterNameOUT = "counter_OUT";
        int initialValue = Integer.parseInt(args[0]);

        for (int i = 0; i < 3; i++) {
            // ------------------------------------------------------------------------
            // Write value
            FileOutputStream fos = null;
            FileOutputStream fos2 = null;
            try {
                fos = new FileOutputStream(counterName);
                fos2 = new FileOutputStream(counterNameIN);
                fos.write(initialValue);
                fos2.write(initialValue);
                System.out.println("Initial counter value is " + initialValue);
            } catch (IOException ioe) {
                ioe.printStackTrace();
                System.exit(-1);
            } finally {
                try {
                    fos.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                try {
                    fos2.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            // ------------------------------------------------------------------------
            // Execute increment
            SimpleImpl.increment(counterName);
            SimpleImpl.increment2(counterNameIN, counterNameOUT);
            // ------------------------------------------------------------------------
            // Read new value
            System.out.println("After Sending task");

            File fIN = new File(counterNameIN);
            File fOUT = new File(counterNameOUT);
            if (i == 0) {
                File f = new File(counterName);
                f.delete();
            }
            fIN.delete();
            fOUT.delete();

        }
        try {
            FileInputStream fis = new FileInputStream(counterName);
            System.out.println("Final counter value is " + fis.read());
            fis.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
        File f = new File(counterName);
        f.delete();

    }

}
