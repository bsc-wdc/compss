package blast;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class BlastImpl {

    public static void assemblyPartitions(String partialFileA, String partialFileB) {
        System.out.println("Assembling partial outputs -> " + partialFileA + " to " + partialFileB);

        String line = null;
        boolean append = true;
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(partialFileA, append));
                BufferedReader bfB = new BufferedReader(new FileReader(partialFileB))) {

            while ((line = bfB.readLine()) != null) {
                bw.write(line);
                bw.newLine();
            }
        } catch (IOException ioe) {
            System.err.println("ERROR: Exception assembling partitions");
            ioe.printStackTrace();
        }
    }

}
