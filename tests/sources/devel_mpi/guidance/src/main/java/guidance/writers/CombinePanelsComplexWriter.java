package guidance.writers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Hashtable;


public class CombinePanelsComplexWriter extends Writer {

    public CombinePanelsComplexWriter(String outputFilePath) {
        super(outputFilePath);
    }

    @Override
    protected void writeHeader(BufferedWriter bw) throws IOException {
        // Nothing to do
    }

    @Override
    protected void writeReversedValues(BufferedWriter bw, Hashtable<Integer, String> reversedTable) throws IOException {
        for (int index = 0; index < reversedTable.size(); index++) {
            String valueReversed = reversedTable.get(index);
            bw.write(valueReversed + "\t");
        }

        bw.newLine();
    }

}
