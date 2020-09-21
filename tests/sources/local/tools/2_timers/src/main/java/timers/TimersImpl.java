package timers;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;


public class TimersImpl {

    public static void increment(String counterFile) {
        int count = -1;

        try (FileInputStream fis = new FileInputStream(counterFile)) {
            count = fis.read();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }

        try (FileOutputStream fos = new FileOutputStream(counterFile)) {
            fos.write(++count);
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

}
