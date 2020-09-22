package utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class FileContentManagement {

    public static void writeValueToFile(String filePath, int value) {
        try (BufferedWriter br = new BufferedWriter(new FileWriter(new File(filePath)))) {
            String valueString = String.valueOf(value) + "\n";
            br.write(String.valueOf(valueString));
        } catch (IOException ioe) {
            System.err.println("ERROR: Exception writing file");
            ioe.printStackTrace();
        }
    }

    public static int readValueFromFile(String filePath) {
        try (BufferedReader br = new BufferedReader(new FileReader(new File(filePath)))) {
            String valueString = br.readLine();
            int val = Integer.valueOf(valueString.trim());
            return val;
        } catch (IOException ioe) {
            System.err.println("ERROR: Exception writing file");
            ioe.printStackTrace();
        }

        return -1;
    }

}
