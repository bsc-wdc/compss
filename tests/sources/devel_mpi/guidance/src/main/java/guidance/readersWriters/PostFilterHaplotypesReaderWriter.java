package guidance.readersWriters;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;


public class PostFilterHaplotypesReaderWriter extends ReaderWriter {

    public PostFilterHaplotypesReaderWriter(String inputFilePath, String outputFilePath, boolean isInputGz, boolean append) {
        super(inputFilePath, outputFilePath, isInputGz, append);
    }

    @Override
    public void readWrite(BufferedReader br, BufferedWriter bw, Object... extraParams) throws IOException {
        String line = null;
        while ((line = br.readLine()) != null) {
            String[] splittedLine = line.split(" ");
            String rsId = splittedLine[1];

            bw.write(rsId);
            bw.newLine();
        }
        bw.flush();
    }
}
