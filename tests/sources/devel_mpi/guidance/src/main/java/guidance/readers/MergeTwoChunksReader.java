package guidance.readers;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import guidance.utils.HashUtils;


public class MergeTwoChunksReader extends Reader {

    public MergeTwoChunksReader(String filePath, boolean isFileGz) {
        super(filePath, isFileGz);
    }

    @Override
    protected List<Integer> constructIndexes(String header, Hashtable<String, Integer> hashTableIndex, Object... extraParams) {
        String newHeader = (String) extraParams[0];

        // The header of the file is omited, we create a new header
        Hashtable<String, Integer> reduceFileAHashTableIndex = HashUtils.createHashWithHeader(newHeader, "\t");
        int indexPosition = reduceFileAHashTableIndex.get("position");
        int indexRsId = reduceFileAHashTableIndex.get("rs_id_all");

        List<Integer> indexes = new LinkedList<Integer>();
        indexes.add(indexPosition);
        indexes.add(indexRsId);

        return indexes;
    }

    @Override
    protected void processLine(String line, List<Integer> indexes, TreeMap<String, ArrayList<String>> resultTreeMap,
            Object... extraParams) {

        int indexPosition = indexes.get(0);
        int indexRsId = indexes.get(1);

        ArrayList<String> fileAList = new ArrayList<>();
        // Delimiter I assume a tap.
        String[] splitted = line.split("\t");

        // Store Position:Store rsIDCases:Store infoCases:Store certCases
        fileAList.add(line);
        String positionAndRsId = splitted[indexPosition] + "_" + splitted[indexRsId];

        // We only store the ones that are not repeated.
        // Question for Siliva: what is the criteria?
        if (!resultTreeMap.containsKey(positionAndRsId)) {
            // Now, we put this casesList into the treemap with the key position
            resultTreeMap.put(positionAndRsId, fileAList);
        } else {
            resultTreeMap.remove(positionAndRsId);
        }
    }

}
