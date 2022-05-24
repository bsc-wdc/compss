package checkpointSnapshot;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import es.bsc.compss.api.COMPSs;


public class Simple {

    private static final String fileName = "counter";
    private static final String fileName2 = "counter1";


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

        try {
            FileOutputStream fos = new FileOutputStream(fileName2);
            fos.write(initialValue);
            fos.close();
            System.out.println("Initial counter value is " + initialValue);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
        // Execute increment
        SimpleImpl.increment(fileName2);
        SimpleImpl.increment(fileName2);
        SimpleImpl.increment(fileName);
        SimpleImpl.increment(fileName);

        int val = 0;
        try {
            FileInputStream fis = new FileInputStream(fileName);
            val = fis.read();
            fis.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
        val = val + 2;
        System.out.println("Value " + val);

        if (exception == 1) {
            COMPSs.snapshot();
            Thread.sleep(5_000);
            throw new Exception("Incorrect number of writers ");
        }

        SimpleImpl.increment(fileName);
        SimpleImpl.increment(fileName);
        SimpleImpl.increment(fileName);
        SimpleImpl.increment(fileName2);
        val = val + 1;
        System.out.println("Value " + val);

        COMPSs.snapshot();
        Thread.sleep(5_000);

        try {
            FileInputStream fis = new FileInputStream(fileName);
            int finalValue = fis.read();
            fis.close();
            FileInputStream fis2 = new FileInputStream(fileName2);
            int finalValue2 = fis2.read();
            fis2.close();
            System.out.print("Final counter value is " + finalValue + " and: " + finalValue2);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

}
