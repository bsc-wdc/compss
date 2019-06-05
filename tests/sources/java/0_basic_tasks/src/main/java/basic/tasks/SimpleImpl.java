package basic.tasks;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;


public class SimpleImpl {

    /**
     * Increases the value of a given counter.
     * 
     * @param counterFile File path to the counter file.
     * @throws FileNotFoundException When file is not found.
     * @throws IOException When file cannot be read or written.
     * @throws Exception When filename is different from the original name
     */
    public static void increment(String counterFile) throws FileNotFoundException, IOException, Exception {
        // Checking if fileName is the original
        File f = new File(counterFile);
        if (!f.getName().equals(Simple.COUNTER_NAME)) {
            throw new Exception(f.getName() + " is different from the original name " + Simple.COUNTER_NAME);
        }
        try {
            FileInputStream fis = new FileInputStream(counterFile);
            int count = fis.read();
            fis.close();
            FileOutputStream fos = new FileOutputStream(counterFile);
            fos.write(++count);
            fos.close();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            throw fnfe;
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw ioe;
        }
    }

}
