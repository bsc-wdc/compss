package checkpointPolicyEvery15secondsDeadline;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class Simple {

    private static final String fileName = "counter";


    public static void main(String[] args) throws Exception {
        // Check and get parameters
        int initialValue = 1;
        if (args.length != 1) {
            throw new Exception("[ERROR] Incorrect number of parameters");
        }
        int exception = Integer.parseInt(args[0]);
        // Write value
        try {
            FileOutputStream fos = new FileOutputStream(fileName);
            fos.write(initialValue);
            fos.close();
            System.out.println("Initial counter value is " + initialValue);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }

        // Execute increment
        SimpleImpl.increment(fileName);
        Thread.sleep(3000);
        SimpleImpl.increment(fileName);
        Thread.sleep(3000);
        SimpleImpl.increment(fileName);
        Thread.sleep(3000);

        if (exception == 1) {
            Thread.sleep(10000);
            throw new Exception("Incorrect number of writers ");
        }

        SimpleImpl.increment(fileName);
        Thread.sleep(3000);
        SimpleImpl.increment(fileName);
        Thread.sleep(3000);
        SimpleImpl.increment(fileName);
        Thread.sleep(3000);
        SimpleImpl.increment(fileName);
        Thread.sleep(3000);
        SimpleImpl.increment(fileName);

        // Write new value

        try {
            FileInputStream fis2 = new FileInputStream(fileName);
            int finalValue = fis2.read();
            fis2.close();
            System.out.println("Final counter value is " + finalValue);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

}
