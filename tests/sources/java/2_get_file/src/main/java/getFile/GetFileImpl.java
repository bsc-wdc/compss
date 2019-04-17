package getFile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileReader;


public class GetFileImpl {

    public static void writeInFile(String file, int i) {
        String str = "New writter type INOUT";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
            writer.write(str + " " + String.valueOf(i) + "\n");
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }

    public static void readInFile(String filename) {
        try (BufferedReader br = new BufferedReader(new FileReader(new File(filename)))) {
            String st;
            while ((st = br.readLine()) != null) {
                System.out.println(st);
            }
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }
}
