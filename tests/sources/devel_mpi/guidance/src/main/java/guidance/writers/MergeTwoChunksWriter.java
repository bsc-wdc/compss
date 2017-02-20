package guidance.writers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Hashtable;


public class MergeTwoChunksWriter extends Writer {

    public MergeTwoChunksWriter(String outputFilePath) {
        super(outputFilePath);
    }

    @Override
    protected void writeHeader(BufferedWriter bw) throws IOException {
    }

    @Override
    protected void writeReversedValues(BufferedWriter bw, Hashtable<Integer, String> reversedTable) throws IOException {
        int index;
        for (index = 0; index < reversedTable.size() - 1; index++) {
            String valueReversed = reversedTable.get(index);
            bw.write(valueReversed + "\t");
        }
        String valueReversed = reversedTable.get(index);
        bw.write(valueReversed);

        bw.newLine();
    }

}
