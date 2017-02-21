package guidance.readers;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;


public class ImputeFileReader extends Reader {

    public ImputeFileReader(String filePath, boolean isFileGz) {
        super(filePath, isFileGz);
    }

    @Override
    protected List<Integer> constructIndexes(String header, Hashtable<String, Integer> hashTableIndex, Object... extraParams) {
        int indexPosition = hashTableIndex.get("position");
        int indexRsId = hashTableIndex.get("rs_id");
        int indexInfo = hashTableIndex.get("info");
        int indexCertainty = hashTableIndex.get("certainty");

        // System.out.println("indexPosition: "+indexPosition);
        // System.out.println("indexRsId: " + indexRsId);
        // System.out.println("indexInfo: " + indexInfo);
        // System.out.println("indexCertainty: " + indexCertainty);

        List<Integer> indexes = new LinkedList<Integer>();
        indexes.add(indexPosition);
        indexes.add(indexRsId);
        indexes.add(indexInfo);
        indexes.add(indexCertainty);

        return indexes;
    }

    @Override
    protected void processLine(String line, List<Integer> indexes, TreeMap<String, ArrayList<String>> resultTreeMap,
            Object... extraParams) {
        
        // Assume single space as delimiter
        String[] splitted = line.split(" ");

        // Store Position:Store rsIDCases:Store infoCases:Store certCases
        ArrayList<String> storeList = new ArrayList<>();
        for (Integer index : indexes) {
            storeList.add(splitted[index]);
        }
        String positionAndRsId = splitted[indexes.get(0)] + "_" + splitted[indexes.get(1)];

        // If there is not a previous snp with this combination of position and rsID, we store it.
        if (!resultTreeMap.containsKey(positionAndRsId)) {
            // We, put this in the resultTreeMap
            resultTreeMap.put(positionAndRsId, storeList);
        } else {
            // If there is a snp with this combination we should remove it
            resultTreeMap.remove(positionAndRsId);
        }
    }

}
