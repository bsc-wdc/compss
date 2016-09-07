package sharedDisks;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;


public class SharedDisksImpl {

    public static int inputTask(String fileName, String name) {
        System.out.println("Reading file");
        try {
            FileInputStream fis = new FileInputStream(fileName);
            System.out.println("--Value: " + String.valueOf(fis.read()));
            fis.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        File f1 = new File(fileName);
        File f2 = new File(name);
        System.out.println("Checking filename");
        System.out.println("--Used file name: " + f1.getAbsolutePath());
        System.out.println("--Original file name: " + f2.getAbsolutePath());

        if (f1.getAbsolutePath().equals(f2.getAbsolutePath())) {
            return 0;
        } else {
            return -1;
        }
    }

    public static void outputTaskWriter(String fileName) {
        try {
            FileOutputStream fos = new FileOutputStream(fileName, true);
            for (int i = 1; i <= 5; ++i) {
                System.out.println("OUT value: " + i);
                fos.write(i);
            }
            fos.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static void outputTaskReader(String fileName) {
        try {
            FileInputStream fis = new FileInputStream(fileName);
            int value = fis.read();
            while (value != -1) {
                System.out.println("IN value: " + value);
                value = fis.read();
            }
            fis.close();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

}
