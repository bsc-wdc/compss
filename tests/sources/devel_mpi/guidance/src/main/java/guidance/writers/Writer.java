package guidance.writers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.Entry;

import guidance.exceptions.GuidanceTaskException;


public abstract class Writer {

    private final String outputFilePath;


    public Writer(String outputFilePath) {
        this.outputFilePath = outputFilePath;
    }

    public void write(boolean append, Hashtable<Integer, String> reversedTable, TreeMap<String, ArrayList<String>> myTree)
            throws GuidanceTaskException {

        if (!append) {
            // Initialize file
            File outputFile = new File(outputFilePath);
            try {
                boolean success = outputFile.createNewFile();
                if (!success) {
                    throw new GuidanceTaskException(
                            "[ERROR] Cannot create output file on " + outputFilePath + ". CreateNewFile returned non-zero exit value");
                }
            } catch (IOException ioe) {
                throw new GuidanceTaskException("[ERROR] Cannot create output file on " + outputFilePath, ioe);
            }
        }

        // Write file content
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(this.outputFilePath))) {
            if (!append) {
                // Writer header on file
                writeHeader(bw);

                // Write reversed values
                writeReversedValues(bw, reversedTable);
            }

            // Write set values
            writeTree(bw, myTree);

            bw.flush();
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot write output file", ioe);
        }
    }

    protected abstract void writeHeader(BufferedWriter bw) throws IOException;

    protected abstract void writeReversedValues(BufferedWriter bw, Hashtable<Integer, String> reversedTable) throws IOException;

    private void writeTree(BufferedWriter bw, TreeMap<String, ArrayList<String>> myTree) throws IOException {
        Iterator<Entry<String, ArrayList<String>>> iter = myTree.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String, ArrayList<String>> m = iter.next();

            ArrayList<String> lineTmp = m.getValue();
            bw.write(lineTmp.get(0));
            for (int j = 1; j < lineTmp.size(); j++) {
                bw.write("\t" + lineTmp.get(j));
            }

            bw.newLine();
        }
    }

}
