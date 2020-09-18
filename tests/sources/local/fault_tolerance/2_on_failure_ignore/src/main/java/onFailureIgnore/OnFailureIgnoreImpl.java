package onFailureIgnore;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class OnFailureIgnoreImpl {

    public static int processParam(String filename) throws numberException {
        File file = new File(filename);
        BufferedReader br;
        String st = "";
        try {
            // Read file contents
            br = new BufferedReader(new FileReader(file));
            st = br.readLine();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Content of file to integer
        int n = Integer.parseInt(st);
        System.out.println("The number readed is : " + n);

        // The written string is the following number
        String str = String.valueOf(n + 1);
        BufferedWriter writer;
        try {
            // Erase file previous contents
            new FileOutputStream(filename).close();

            // Write the new number to the file
            writer = new BufferedWriter(new FileWriter(file, false));
            writer.write(str);
            writer.close();
            System.out.println("Writing to file");

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("The written number is : " + str);
        return n;
    }

    // Function called if the execution has to continue
    private static void failOnce(int n) throws numberException {
        // Exception thrown
        if (n == 1) {
            throw new numberException("The number is too low");
        }
    }

    public static void processParamIgnoreFailure(String filename) throws numberException {
        int n = processParam(filename);
        failOnce(n);
    }

    public static void processOutParamIgnoreFailure(String fileoutName1, String fileOut2) throws numberException {

    }
}
