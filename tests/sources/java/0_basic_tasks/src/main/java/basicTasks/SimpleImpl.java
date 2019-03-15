package basicTasks;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;


public class SimpleImpl {

    public static void increment(String counterFile) throws Exception {
        //Checking if fileName is the original
        File f = new File(counterFile);
        if (!f.getName().equals(Simple.COUNTER_NAME)){
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
