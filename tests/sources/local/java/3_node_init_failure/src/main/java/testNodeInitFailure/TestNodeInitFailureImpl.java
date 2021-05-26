package testNodeInitFailure;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;


public class TestNodeInitFailureImpl {

    public static void increment(String counterFile, String out) {
        try {
            FileInputStream fis = new FileInputStream(counterFile);
            int initial = fis.read();
            fis.close();

            int nm = (new Random()).nextInt(7);
            if (nm < 2) {
                System.exit(1);
            }
            Thread.sleep(10000);
            FileOutputStream fos = new FileOutputStream(out);
            fos.write(initial + 1);
            fos.close();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}