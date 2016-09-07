package advancedTracing;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;


public class TracingImpl {

    public static void task1() {
        System.out.println("[LOG] Task 1: Hello");
    }

    public static void task2() {
        System.out.println("[LOG] Task 2: Hello");
    }

    public static void task3() {
        System.out.println("[LOG] Task 3: Hello");
    }

    public static Integer task4(String counterFile) {
        System.out.println("[LOG] Task 4: Hello");
        int count = -1;
        try {
            FileInputStream fis = new FileInputStream(counterFile);
            count = fis.read();
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
        return count;
    }
}
