package basicTasks;

import java.io.FileInputStream;
import java.io.IOException;


public class Reader {

    public static void checkResult(String counterName) {
        System.out.println("External class checking result");
        try {
            FileInputStream fis = new FileInputStream(counterName);
            System.out.println("Final counter 3 value is " + fis.read());
            fis.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

}
