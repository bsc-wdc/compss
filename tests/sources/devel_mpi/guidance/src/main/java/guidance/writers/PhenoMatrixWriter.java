package guidance.writers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Hashtable;


public class PhenoMatrixWriter extends Writer {

    public PhenoMatrixWriter(String outputFilePath) {
        super(outputFilePath);
    }

    @Override
    protected void writeHeader(BufferedWriter bw) throws IOException {
        bw.write("chr\tposition");
    }

    @Override
    protected void writeReversedValues(BufferedWriter bw, Hashtable<Integer, String> reversedTable) throws IOException {
        // Nothing to do
    }

}
