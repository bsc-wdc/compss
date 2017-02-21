package guidance.readers;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import guidance.utils.HashUtils;


public class PartialPhenomeFileReader extends Reader {

    public PartialPhenomeFileReader(String filePath, boolean isFileGz) {
        super(filePath, isFileGz);
    }

    @Override
    protected List<Integer> constructIndexes(String header, Hashtable<String, Integer> hashTableIndex, Object... extraParams) {
        String phenomeHeader = "chr\tposition";
        Hashtable<String, Integer> phenomeHashTableIndex = HashUtils.createHashWithHeader(phenomeHeader, "\t");

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

        ArrayList<String> firstList = new ArrayList<String>();
        // delimiter,I assume TAP space.
        String[] splitted = line.split("\t");

        String chrAndPosition = splitted[indexChrInPhenomeAFile] + "_" + splitted[indexPositionInPhenomeAFile];

        // Store chr:position
        firstList.add(splitted[indexChrInPhenomeAFile]);
        firstList.add(splitted[indexPositionInPhenomeAFile]);

        // Finally, we put this data into the phenomeATreeMap, using chrPosition as key and the firstList as value.
        resultTreeMap.put(chrAndPosition, firstList);
    }

}
