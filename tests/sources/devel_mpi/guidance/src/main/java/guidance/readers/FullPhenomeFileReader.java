package guidance.readers;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import guidance.utils.HashUtils;


public class FullPhenomeFileReader extends Reader {

    public FullPhenomeFileReader(String filePath, boolean isFileGz) {
        super(filePath, isFileGz);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected List<Integer> constructIndexes(String header, Hashtable<String, Integer> hashTableIndex, Object... extraParams) {
        boolean withHeader = (boolean) extraParams[0];
        String ttName = (String) extraParams[1];
        String rpName = (String) extraParams[2];
        Hashtable<Integer, String> phenomeHashTableIndexReversedInput = (Hashtable<Integer, String>) extraParams[3];

        if (!withHeader) {
            // Change the header
            header = "chr\tposition";
            header = header + "\t" + ttName + ":" + rpName + ":" + "rs_id_all";
            header = header + "\t" + ttName + ":" + rpName + ":" + "alleleA";
            header = header + "\t" + ttName + ":" + rpName + ":" + "alleleB";
            header = header + "\t" + ttName + ":" + rpName + ":" + "all_maf";
            header = header + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_pvalue";
            header = header + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_beta_1";
            header = header + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_se_1";

            header = header + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_beta_1:genotype/sex=1";
            header = header + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_beta_2:genotype/sex=2";
            header = header + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_se_1:genotype/sex=1";
            header = header + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_se_2:genotype/sex=2";
        }

        Hashtable<String, Integer> phenomeHashTableIndex = HashUtils.createHashWithHeader(header, "\t");
        Hashtable<Integer, String> phenomeHashTableIndexReversed = HashUtils.createHashWithHeaderReversed(header, "\t");
        phenomeHashTableIndexReversedInput.putAll(phenomeHashTableIndexReversed);

        int indexChrInPhenomeAFile = phenomeHashTableIndex.get("chr");
        int indexPositionInPhenomeAFile = phenomeHashTableIndex.get("position");

        List<Integer> indexes = new LinkedList<Integer>();
        indexes.add(indexChrInPhenomeAFile);
        indexes.add(indexPositionInPhenomeAFile);

        return indexes;
    }

    @Override
    protected void processLine(String line, List<Integer> indexes, TreeMap<String, ArrayList<String>> resultTreeMap,
            Object... extraParams) {
        
        int indexChrInPhenomeAFile = indexes.get(0);
        int indexPositionInPhenomeAFile = indexes.get(1);

        ArrayList<String> currentList = new ArrayList<String>();
        // delimiter,I assume TAB space.
        String[] splitted = line.split("\t");

        String chrAndPosition = splitted[indexChrInPhenomeAFile] + "_" + splitted[indexPositionInPhenomeAFile];
        for (int i = 0; i < splitted.length; i++) {
            currentList.add(splitted[i]);
        }

        // We update the phenomeATreeMap with the currentList
        resultTreeMap.put(chrAndPosition, currentList);
    }

}
