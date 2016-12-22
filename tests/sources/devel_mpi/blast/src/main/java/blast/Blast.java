package blast;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.UUID;

import binary.BINARY;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import blast.BlastImpl;
import blast.exceptions.BlastException;


public class Blast {

    private static final String ENV_BLAST_BINARY = "BLAST_BINARY";

    private static boolean debug;
    private static String databasePath;
    private static String databaseName;
    private static String inputFileName;
    private static int numFragments;
    private static String tmpDir;
    private static String outputFileName;
    private static String commandArgs;

    private static List<String> partialOutputs = null;
    private static List<String> partialInputs = null;


    public static void main(String[] args) throws BlastException {
        // Parse application parameters
        parseArgs(args);

        // Log execution
        logArgs();

        // -------------------------------------------------------
        // Start execution
        Long startTotalTime = System.currentTimeMillis();

        try {
            // Split sequence input file
            splitSequenceFile();
    
            // Submit tasks
            alignSequences();
    
            // Assembly process
            String lastMerge = assembleSequences();
    
            // Move result to expected output file
            moveResult(lastMerge);
        } catch (BlastException be) {
            throw be;
        } finally {
            // Clean up partial results
            cleanUp();
        }

        // -------------------------------------------------------
        // Log timers
        Long stopTotalTime = System.currentTimeMillis();
        Long totalTime = (stopTotalTime - startTotalTime) / 1000;
        System.out.println("- " + Blast.inputFileName + " sequences aligned successfully in " + totalTime + " seconds");
        System.out.println("");
    }

    private static void parseArgs(String[] args) {
        Blast.debug = Boolean.parseBoolean(args[0]);
        Blast.databasePath = args[1];
        Blast.inputFileName = args[2];
        Blast.numFragments = Integer.parseInt(args[3]);
        Blast.tmpDir = args[4];
        Blast.outputFileName = args[5];

        commandArgs = " ";
        for (int i = 6; i < args.length; i++) {
            commandArgs += args[i] + " ";
        }

        // Parsing database name
        // Splitting the files model path string using a forward slash as delimiter
        StringTokenizer st = new StringTokenizer(Blast.databasePath, "/");
        Blast.databaseName = null;
        while (st.hasMoreElements()) {
            Blast.databaseName = st.nextToken();
        }
    }

    private static void logArgs() {
        System.out.println("BLAST Sequence Alignment Tool");
        System.out.println("");

        System.out.println("Parameters: ");
        System.out.println("- Blast binary: " + System.getenv(ENV_BLAST_BINARY));
        System.out.println("- Debug: " + Blast.debug);
        System.out.println("- Database Name with Path: " + Blast.databasePath);
        System.out.println("- Database Name: " + Blast.databaseName);
        System.out.println("- Input Sequences File: " + Blast.inputFileName);
        System.out.println("- Number of expected fragments: " + Blast.numFragments);
        System.out.println("- Temporary Directory: " + Blast.tmpDir);
        System.out.println("- Output File: " + Blast.outputFileName);
        System.out.println("- Command Line Arguments: " + Blast.commandArgs);
        System.out.println("");
    }

    private static void splitSequenceFile() throws BlastException {
        System.out.println("Split sequence file");
        
        // Read number of different sequences
        int nsequences = 0;
        try (BufferedReader bf = new BufferedReader(new FileReader(Blast.inputFileName))) {
            String line = null;
            while ((line = bf.readLine()) != null) {
                if (line.contains(">")) {
                    nsequences++;
                }
            }
        } catch (IOException ioe) {
            String msg = "ERROR: Cannot read input file " + Blast.inputFileName;
            System.err.print(msg);
            throw new BlastException(msg, ioe);
        }

        System.out.println("- The total number of sequences is: " + nsequences);

        // Calculate seqs per fragment and needed files
        int seqsPerFragment = (int) Math.round(((double) nsequences / (double) Blast.numFragments));
        Blast.partialInputs = new ArrayList<String>(Blast.numFragments);
        Blast.partialOutputs = new ArrayList<String>(Blast.numFragments);

        if (Blast.debug) {
            System.out.println("- The total number of sequences of a fragment is: " + seqsPerFragment);
            System.out.println("\n- Splitting sequences among fragment files...");
        }

        // Split into files
        int frag = 0;
        boolean append = true;
        BufferedWriter bw = null;
        try (BufferedReader bf = new BufferedReader(new FileReader(Blast.inputFileName))) {
            String line = null;
            while ((line = bf.readLine()) != null) {
                if (line.contains(">")) {
                    if (bw != null) {
                        bw.close();
                    }
                    if (frag < Blast.numFragments) {
                        // Creating fragment
                        UUID index = UUID.randomUUID();
                        String partitionFile = Blast.tmpDir + "seqFile" + index + ".sqf";
                        String partitionOutput = Blast.tmpDir + "resFile" + index + ".result.txt";
                        // Touch output file
                        new FileOutputStream(partitionOutput).close();
                        // Store fileNames
                        Blast.partialInputs.add(partitionFile);
                        Blast.partialOutputs.add(partitionOutput);
                    }
                    // Preparing for writing to next fragment
                    bw = new BufferedWriter(new FileWriter(Blast.partialInputs.get((frag % Blast.numFragments)), append));
                    frag++;
                }
                bw.write(line);
                bw.newLine();
            }
        } catch (IOException ioe) {
            String msg = "ERROR: Cannot read input file " + Blast.inputFileName;
            System.err.print(msg);
            throw new BlastException(msg, ioe);
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException ioe) {
                    String msg = "ERROR: Cannot close BW";
                    System.err.print(msg);
                    throw new BlastException(msg, ioe);
                }
            }
        }        
        
        if (Blast.debug) {
            System.out.println("Input Files are: ");
            for (int i = 0; i < Blast.partialInputs.size(); ++i) {
                System.out.println("   - " + Blast.partialInputs.get(i));
            }
            System.out.println("Output Files are: ");
            for (int i = 0; i < Blast.partialOutputs.size(); ++i) {
                System.out.println("   - " + Blast.partialOutputs.get(i));
            }
        }
    }

    /**
     * Creates MAP Tasks
     */
    private static void alignSequences() throws BlastException {
        System.out.println("");
        System.out.println("Aligning Sequences:");

        String pParam = "-p blastx";
        String dbParam = "-d " + Blast.databaseName;
        String inputFlag = "-i ";
        String outputFlag = "-o";
        int numAligns = Blast.partialInputs.size();
        Integer[] exitValues = new Integer[numAligns];
        for (int i = 0; i < numAligns; i++) {
            exitValues[i] = BINARY.align(pParam, dbParam, inputFlag, Blast.partialInputs.get(i), outputFlag, Blast.partialOutputs.get(i),
                    Blast.commandArgs);
        }

        if (Blast.debug) {
            System.out.println("");
            System.out.println(" - Number of fragments to assemble -> " + Blast.partialOutputs.size());
        }

        // Enable this code if you wish to check the binary result (adds synchronization)
        // for (int i = 0; i < numAligns; i++) {
        // if (exitValues[i] != 0) {
        // throw new BlastException("ERROR: Align task " + i + " finished with non-zero value");
        // }
        // }
    }

    /**
     * Creates reduce tasks
     * 
     * @return fileName of last reduce
     */
    private static String assembleSequences() {
        // MERGE-REDUCE
        LinkedList<Integer> q = new LinkedList<Integer>();
        for (int i = 0; i < Blast.partialOutputs.size(); i++) {
            q.add(i);
        }

        int x = 0;
        while (!q.isEmpty()) {
            x = q.poll();
            if (!q.isEmpty()) {
                int y = q.poll();

                if (debug) {
                    System.out.println(" - Merging files -> " + Blast.partialOutputs.get(x) + " and " + Blast.partialOutputs.get(y));
                }
                BlastImpl.assemblyPartitions(Blast.partialOutputs.get(x), Blast.partialOutputs.get(y));
                q.add(x);
            }
        }
        
        return Blast.partialOutputs.get(0);
    }

    private static void moveResult(String resultFile) throws BlastException {
        if (Blast.debug) {
            System.out.println("");
            System.out.println("Moving last merged file: " + resultFile + " to " + Blast.outputFileName);
            System.out.println("");
        }

        try (FileInputStream fis = new FileInputStream(resultFile)) {
            copyFile(fis, new File(Blast.outputFileName));
        } catch (IOException ioe) {
            String msg = "ERROR: Cannot copy file " + resultFile + " to " + Blast.outputFileName;
            System.err.print(msg);
            throw new BlastException(msg, ioe);
        }
    }

    private static void copyFile(FileInputStream sourceFile, File destFile) throws IOException {
        try (FileChannel source = sourceFile.getChannel();
                FileOutputStream outputDest = new FileOutputStream(destFile);
                FileChannel destination = outputDest.getChannel()) {

            destination.transferFrom(source, 0, source.size());
        } catch (IOException ioe) {
            throw ioe;
        }
    }

    private static void cleanUp() {
        // Cleaning intermediate sequence input files
        for (int i = 0; i < Blast.partialInputs.size(); i++) {
            File fSeq = new File(Blast.partialInputs.get(i));
            fSeq.delete();
        }

        for (int i = 0; i < Blast.partialOutputs.size(); i++) {
            File fres = new File(Blast.partialOutputs.get(i));
            fres.delete();
        }
    }

}
