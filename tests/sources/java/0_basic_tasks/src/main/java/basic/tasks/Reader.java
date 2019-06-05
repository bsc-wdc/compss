package basic.tasks;

import java.io.FileInputStream;
import java.io.IOException;


public class Reader {

    /**
     * Prints the counter content of the given file.
     * 
     * @param counterName Counter file name.
     */
    public static void checkResult(String counterName) throws IOException {
        System.out.println("External class checking result");
        try (FileInputStream fis = new FileInputStream(counterName)) {
            System.out.println("Final counter 3 value is " + fis.read());
        }
    }

}
