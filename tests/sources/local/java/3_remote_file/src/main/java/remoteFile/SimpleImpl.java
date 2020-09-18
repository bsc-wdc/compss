package remoteFile;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;


public class SimpleImpl {

    public static void increment(String counterPathIN, String counterPathOUT) {
        try {
            FileInputStream fis = new FileInputStream(counterPathIN);
            int count = fis.read();
            fis.close();
            FileOutputStream fos = new FileOutputStream(counterPathOUT);
            fos.write(++count);
            fos.close();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

}
