package schedulerData;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;


public class MainImpl {

    private static final int SLEEP_TASK = 250; // ms


    public static void increment(String fileInOut, String fileIn) {
        // Perform increment
        try {
            FileInputStream fis = new FileInputStream(fileInOut);
            int count = fis.read();
            fis.close();
            FileOutputStream fos = new FileOutputStream(fileInOut);
            fos.write(++count);
            fos.close();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }

        // Add delay to make task last longer
        try {
            Thread.sleep(SLEEP_TASK);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
