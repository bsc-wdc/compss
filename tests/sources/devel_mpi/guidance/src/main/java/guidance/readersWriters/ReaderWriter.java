package guidance.readersWriters;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import guidance.exceptions.GuidanceTaskException;


public abstract class ReaderWriter {

    protected final String inputFilePath;
    protected final String outputFilePath;
    protected final boolean isInputGz;
    protected final boolean append;


    public ReaderWriter(String inputFilePath, String outputFilePath, boolean isInputGz, boolean append) {
        this.inputFilePath = inputFilePath;
        this.outputFilePath = outputFilePath;
        this.isInputGz = isInputGz;
        this.append = append;
    }

    public void execute(Object... extraParams) throws GuidanceTaskException {
        // Try to create the output file if its not append
        if (!append) {
            File outputFile = new File(outputFilePath);
            try {
                boolean success = outputFile.createNewFile();
                if (!success) {
                    throw new GuidanceTaskException("[ERROR] Cannot create output file. CreateNewFile returned non-zero exit value");
                }
            } catch (IOException ioe) {
                throw new GuidanceTaskException("[ERROR] Cannot create output file", ioe);
            }
            System.out.println("\n[DEBUG] \t- Output file " + outputFilePath + " succesfuly created");
        }

        // Process input

        try (BufferedReader br = (isInputGz)
                ? new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(inputFilePath))))
                : new BufferedReader(new FileReader(inputFilePath));
                BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilePath, append))) {

            readWrite(br, bw, extraParams);
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] processing A file", ioe);
        }
    }

    protected abstract void readWrite(BufferedReader br, BufferedWriter bw, Object... extraParams) throws IOException;

}
