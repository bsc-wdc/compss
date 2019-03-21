package getFile;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileNotFoundException;


public class GetFileImpl {

    public static void writeInFile(String file, int i) {
        try {
            String str = "New writter";
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

}
