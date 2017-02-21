package guidance.readersWriters;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;


public class JointCondensedFilesReaderWriter extends ReaderWriter {

    public JointCondensedFilesReaderWriter(String inputFilePath, String outputFilePath, boolean isInputGz, boolean append) {
        super(inputFilePath, outputFilePath, isInputGz, append);
    }

    @Override
    public void readWrite(BufferedReader br, BufferedWriter bw, Object... extraParams) throws IOException {
        // I read the header
        String line = br.readLine();

        // Put the header in the output file.
        bw.write(line);
        bw.newLine();

        while ((line = br.readLine()) != null) {
            bw.write(line);
            bw.newLine();
        }

        bw.flush();
    }
}
