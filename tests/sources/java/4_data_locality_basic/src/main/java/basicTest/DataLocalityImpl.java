package basicTest;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;


public class DataLocalityImpl {

    public static void task(int id, String fileName) {
        try {
            FileOutputStream fos = new FileOutputStream(fileName, true);
            String value = String.valueOf(id) + "\n";
            fos.write(value.getBytes());
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
