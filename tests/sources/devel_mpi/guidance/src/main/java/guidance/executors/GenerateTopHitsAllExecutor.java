package guidance.executors;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import guidance.utils.HashUtils;


public class GenerateTopHitsAllExecutor extends Executor {

    public GenerateTopHitsAllExecutor(String inputFilePath, String outputFilePath, boolean isInputGz, boolean append) {
        super(inputFilePath, outputFilePath, isInputGz, append);
    }

    @Override
    public void execute(BufferedReader br, BufferedWriter bw, Object... extraParams) throws IOException {
        Double pvaThres = (Double) extraParams[0];

        TreeMap<String, String> fileTreeMap = new TreeMap<String, String>();
        Hashtable<String, Integer> resultsFileHashTableIndex = new Hashtable<String, Integer>();

        // First: read the header and avoid it
        String header = br.readLine();
        resultsFileHashTableIndex = HashUtils.createHashWithHeader(header, "\t");
        int indexPosition = resultsFileHashTableIndex.get("position");
        int indexRsId = resultsFileHashTableIndex.get("rs_id_all");
        int indexPvalue = resultsFileHashTableIndex.get("frequentist_add_pvalue");
        int indexChromo = resultsFileHashTableIndex.get("chr");
        int indexAllMaf = resultsFileHashTableIndex.get("all_maf");
        int indexAlleleA = resultsFileHashTableIndex.get("alleleA");
        int indexAlleleB = resultsFileHashTableIndex.get("alleleB");

        String line = null;
        while ((line = br.readLine()) != null) {
            String[] splitted = line.split("\t");
            String positionAndRsId = splitted[indexPosition] + "_" + splitted[indexRsId];
            double myPva = Double.parseDouble(splitted[indexPvalue]);

            if (myPva <= pvaThres && myPva > 0.0) {
                // Now, we put this String into the treemap with the key positionAndRsId
                // reducedLine is chr;position;RSID_ALL;MAF;a1;a2;pval
                String reducedLine = splitted[indexChromo] + "\t" + splitted[indexPosition] + "\t" + splitted[indexRsId] + "\t"
                        + splitted[indexAllMaf] + "\t" + splitted[indexAlleleA] + "\t" + splitted[indexAlleleB] + "\t"
                        + splitted[indexPvalue];
                fileTreeMap.put(positionAndRsId, reducedLine);
            }
        }

        if (!append) {
            String newHeader = "chr\tposition\trsid\tMAF\ta1\ta2\tpval_add";
            bw.write(newHeader);
        }
        Set<Entry<String, String>> mySet = fileTreeMap.entrySet();
        // Move next key and value of Map by iterator
        Iterator<Entry<String, String>> iter = mySet.iterator();
        while (iter.hasNext()) {
            // key=value separator this by Map.Entry to get key and value
            Entry<String, String> m = iter.next();
            // getKey is used to get key of Map
            String myLine = (String) m.getValue();

            bw.newLine();
            bw.write(myLine);
        }

        bw.flush();
    }
}
