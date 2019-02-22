package testConcurrent;

//import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.Thread;


public class TestConcurrentImpl {

    private static final int TASK_SLEEP_TIME = 2_000; // ms


    public static void writeOne(String fileName) {
        try (FileOutputStream fos = new FileOutputStream(fileName, true)) {
            fos.write(1);
            Thread.sleep(TASK_SLEEP_TIME);
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void writeTwo(String fileName) {
        try (FileOutputStream fos = new FileOutputStream(fileName, true)) {
            fos.write(2);
            Thread.sleep(TASK_SLEEP_TIME);
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
