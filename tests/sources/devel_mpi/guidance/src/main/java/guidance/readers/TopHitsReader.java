package guidance.readers;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;


public class TopHitsReader extends Reader {

    public TopHitsReader(String filePath, boolean isFileGz) {
        super(filePath, isFileGz);
    }

    @Override
    protected List<Integer> constructIndexes(String header, Hashtable<String, Integer> hashTableIndex, Object... extraParams) {
        int indexChrInTopHitsFile = hashTableIndex.get("chr");
        int indexPositionInTopHitsFile = hashTableIndex.get("position");

        List<Integer> indexes = new LinkedList<Integer>();
        indexes.add(indexChrInTopHitsFile);
        indexes.add(indexPositionInTopHitsFile);

        return indexes;
    }

    @Override
    protected void processLine(String line, List<Integer> indexes, TreeMap<String, ArrayList<String>> resultTreeMap,
            Object... extraParams) {
        
        int indexChrInTopHitsFile = indexes.get(0);
        int indexPositionInTopHitsFile = indexes.get(1);

        ArrayList<String> firstList = new ArrayList<String>();
        // delimiter I assume TAP space.
        String[] splitted = line.split("\t");
        String chrAndPosition = splitted[indexChrInTopHitsFile] + "_" + splitted[indexPositionInTopHitsFile];

        // Store chr:position:rsId:pvalues for the all combination of phenotypes and panels
        firstList.add(splitted[indexChrInTopHitsFile]);
        firstList.add(splitted[indexPositionInTopHitsFile]);

        // System.out.println("\n[DEBUG] phenomeHashTableIndex.size() " + phenomeHashTableIndex.size());

        // Finally, we put this data into the firstTreeMap, using chrPosition as key and the firstList as value.
        resultTreeMap.put(chrAndPosition, firstList);
    }

}
