package guidance.writers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Hashtable;


public class CollectSummaryWriter extends Writer {

    public CollectSummaryWriter(String outputFilePath) {
        super(outputFilePath);
    }

    @Override
    protected void writeHeader(BufferedWriter bw) throws IOException {
        bw.write("chr\tposition\trs_id_all\tinfo_all\tcertainty_all\t");
    }

    @Override
    protected void writeReversedValues(BufferedWriter bw, Hashtable<Integer, String> reversedTable) throws IOException {
        // We do not store the first 4 field because they are not necessary or are repeated:
        // These four fields are:
        // alternative_ids, rsid, chromosome, position
        for (int index = 4; index < reversedTable.size(); index++) {
            String valueReversed = reversedTable.get(index);
            bw.write(valueReversed + "\t");
        }

        bw.newLine();
    }

}
