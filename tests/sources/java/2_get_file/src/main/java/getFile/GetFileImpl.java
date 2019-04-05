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
        try {
            String str = "New writter type INOUT";
            BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
            writer.write(str + " " + String.valueOf(i) + "\n");

            writer.close();
        } catch (FileNotFoundException fnfe) {
            fnfe.printStackTrace();
            System.exit(-1);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(-1);
        }
    }
    public static void readInFile(String filename) {
        try {
            File file = new File(filename); 
            BufferedReader br = new BufferedReader(new FileReader(file)); 
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
