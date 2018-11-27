package nonNativeTasks;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class FileDumper {

    public static void dumpFile(String title, String file) throws IOException {
        System.out.println(title);
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException ioe) {
            throw ioe;
        }
    }
}