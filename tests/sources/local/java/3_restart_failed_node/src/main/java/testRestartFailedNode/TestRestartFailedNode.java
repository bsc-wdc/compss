package testRestartFailedNode;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class TestRestartFailedNode {

    public static void main(String[] args) throws Exception {
        System.out.println("[LOG] Main program started.");

        String counterName = "counter";
        String countOut = "counter_out";
        String initialValue = "2";
        try {
            FileOutputStream fos = new FileOutputStream(counterName);
            fos.write(Integer.parseInt(initialValue));
            fos.close();
            FileInputStream fis = new FileInputStream(counterName);
            System.out.println("Initial counter value is " + fis.read());
            fis.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        for (int i = 0; i < 10; i++) {
            TestRestartFailedNodeImpl.increment(counterName, countOut);
            String tmp = counterName;
            counterName = countOut;
            countOut = tmp;
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            FileInputStream fis = new FileInputStream(counterName);
            System.out.println("Final counter value is " + fis.read());
            fis.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        System.out.println("[LOG] Main program finished.");
    }

}
