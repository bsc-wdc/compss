package onFailureInOut;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileReader;


public class OnFailureInOutImpl {

    public static void processParamRead(String filename) throws numberException {
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
        failMultiple(n);
    }

    public static void processParamWrite(String filename, String filename2) throws numberException {
        File file = new File(filename);
        File file2 = new File(filename2);
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
        String str = String.valueOf(1);
        BufferedWriter writer;
        try {

            // Write the new number to the file
            writer = new BufferedWriter(new FileWriter(file2, false));
            writer.write(str);
            writer.close();
            System.out.println("Writing to file");

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("The written number is : " + str);

    }

    // Function called if the execution has to fail multiple times
    private static void failMultiple(int n) throws numberException {
        // Exception thrown
        if (n < 3) {
            throw new numberException("The number is too low");
        }
    }

}
