package simple;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;


public class SimpleGOSImpl {

    public static void increment(String counterFile) {
        System.out.println("++++++++Doing task IMPLEMENTATION++++++++++");
        try {
            FileInputStream fis = new FileInputStream(counterFile);
            int count = fis.read();
            fis.close();
            FileOutputStream fos = new FileOutputStream(counterFile);
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

    /*
     * public static void increment() {
     * 
     * System.out.println("---------------------------Ejecucion de la tarea---------------------------"); try {
     * Thread.sleep(10000); } catch (Exception e) {
     * System.out.println("---------------------------FIN de la tarea---------------------------"); } }
     */

}
