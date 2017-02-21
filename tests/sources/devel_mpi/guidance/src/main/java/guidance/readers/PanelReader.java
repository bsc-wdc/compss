package guidance.readers;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import guidance.utils.HashUtils;


public class PanelReader extends Reader {

    public PanelReader(String filePath, boolean isFileGz) {
        super(filePath, isFileGz);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected List<Integer> constructIndexes(String header, Hashtable<String, Integer> hashTableIndex, Object... extraParams) {
        // String chromoS = (String) extraParams[0];

        Hashtable<String, Integer> phenomeHashTableIndexInput = (Hashtable<String, Integer>) extraParams[1];
        phenomeHashTableIndexInput.putAll(hashTableIndex);

        Hashtable<Integer, String> phenomeHashTableIndexReversedInput = (Hashtable<Integer, String>) extraParams[2];
        Hashtable<Integer, String> phenomeHashTableIndexReversed = HashUtils.createHashWithHeaderReversed(header, "\t");
        phenomeHashTableIndexReversedInput.putAll(phenomeHashTableIndexReversed);

        int chrIdx = hashTableIndex.get("chr");
        int posIdx = hashTableIndex.get("position");
        int a1Idx = hashTableIndex.get("alleleA");
        int a2Idx = hashTableIndex.get("alleleB");
        int infoIdx = hashTableIndex.get("info_all");

        List<Integer> indexes = new LinkedList<Integer>();
        indexes.add(chrIdx);
        indexes.add(posIdx);
        indexes.add(a1Idx);
        indexes.add(a2Idx);
        indexes.add(infoIdx);

        return indexes;
    }

    @Override
    protected void processLine(String line, List<Integer> indexes, TreeMap<String, ArrayList<String>> resultTreeMap,
            Object... extraParams) {
        String chromoS = (String) extraParams[0];
        int chrIdx = indexes.get(0);
        int posIdx = indexes.get(1);
        int a1Idx = indexes.get(2);
        int a2Idx = indexes.get(3);
        // int infoIdx = indexes.get(4);

        String[] splitted = line.split("\t");

        if (splitted[chrIdx].equals(chromoS)) {
            String positionA1A2Chr = splitted[posIdx] + "_" + splitted[a1Idx] + "_" + splitted[a2Idx] + "_" + splitted[chrIdx];
            ArrayList<String> l = new ArrayList<>();
            l.add(line);

            // Now, we put this String into the treemap with the key positionA1A1Chr
            resultTreeMap.put(positionA1A2Chr, l);
        }
    }

}
