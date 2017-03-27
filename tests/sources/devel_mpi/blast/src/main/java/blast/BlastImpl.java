package blast;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import blast.exceptions.BlastException;


public class BlastImpl {

    /**
     * Splits the input file into the fragment myFrag
     * 
     * @param inputFileName
     * @param partitionFile
     * @param nFrags
     * @param myFrag
     * @throws BlastException
     */
    public static void splitPartitions(String inputFileName, String partitionFile, int nFrags, int myFrag) throws BlastException {
        int frag = 0;
        try (BufferedReader bf = new BufferedReader(new FileReader(inputFileName));
                BufferedWriter bw = new BufferedWriter(new FileWriter(partitionFile, true))) {

            String line = null;
            while ((line = bf.readLine()) != null) {
                if (line.contains(">")) {
                    frag++;
                }
                if (frag % nFrags == myFrag) {
                    bw.write(line);
                    bw.newLine();
                }
            }
        } catch (IOException ioe) {
            String msg = "ERROR: Cannot read input file " + inputFileName;
            System.err.print(msg);
            throw new BlastException(msg, ioe);
        }
    }

    /**
     * Assembles the two partial files
     * 
     * @param partialFileA
     * @param partialFileB
     */
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
