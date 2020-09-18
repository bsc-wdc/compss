package testCOMPSsExceptions;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import es.bsc.compss.worker.COMPSsException;


public class TestCOMPSsExceptionsImpl {

    public static void writeOne(String fileName) throws COMPSsException {
        System.out.println("Filename: " + fileName);
        writeFile(fileName, String.valueOf(1));
        System.out.println("1 written");
        String contents = readFile(fileName);
        System.out.println(contents);
        // COMPSsException raised
        throw (new COMPSsException("Exception from write one"));
    }

    public static void writeThree(String fileName) {
        writeFile(fileName, String.valueOf(3));
        System.out.println("3 written");
    }

    public static void writeFour(String fileName) {
        writeFile(fileName, String.valueOf(4));
        System.out.println("4 written");
        String contents = readFile(fileName);
        System.out.println(contents);
    }

    public static void writeFile(String fileName, String i) {
        File f = new File(fileName);

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(f, true));
            writer.write(i);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                // Close the writer regardless of what happens
                writer.close();
            } catch (Exception e) {
            }
        }
    }

    public static String readFile(String fileName) {
        File f = new File(fileName);
        BufferedReader br = null;
        String contents = "";
        try {
            br = new BufferedReader(new FileReader(f));
            String line;
            while ((line = br.readLine()) != null) {
                contents = contents + line;
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return contents;
    }
}