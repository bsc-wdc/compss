package guidance.executors;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;


public class FilterByInfoExecutor extends Executor {

    public FilterByInfoExecutor(String inputFilePath, String outputFilePath, boolean isInputGz, boolean append) {
        super(inputFilePath, outputFilePath, isInputGz, append);
    }

    @Override
    public void execute(BufferedReader br, BufferedWriter bw, Object... extraParams) throws IOException {
        // Retrieve parameters
        int infoIndex = (int) extraParams[0];
        int rsIdIndex = (int) extraParams[1];
        Double thresholdDouble = (Double) extraParams[2];

        // I read the header
        String line = br.readLine();
        while ((line = br.readLine()) != null) {
            String[] splittedLine = line.split(" ");// delimiter I assume single space.
            Double info = Double.parseDouble(splittedLine[infoIndex]); // store info value in Double format

            // Store rsID into filteredFile if info >= threshold
            int retval = Double.compare(info, thresholdDouble);
            if (retval >= 0) {
                // The info value is greater or equal to the threshold, then store the rsID into the output file.
                bw.write(splittedLine[rsIdIndex]);
                bw.newLine();
            }
        }

        bw.flush();
    }
}
