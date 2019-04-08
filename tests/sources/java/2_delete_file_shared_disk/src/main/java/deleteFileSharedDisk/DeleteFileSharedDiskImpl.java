package deleteFileSharedDisk;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;

import java.io.FileNotFoundException;
import java.io.FileReader;


public class DeleteFileSharedDiskImpl {

    public static void readFromFile(String filename) {
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

        System.out.println("The number readed is : " + st);
    }

}
