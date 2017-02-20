package guidance.readers;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import guidance.exceptions.GuidanceTaskException;
import guidance.utils.HashUtils;


public class Reader {

    private final String filePath;
    private final boolean isFileGz;


    public Reader(String filePath, boolean isFileGz) {
        this.filePath = filePath;
        this.isFileGz = isFileGz;
    }

    public TreeMap<String, ArrayList<String>> read() throws GuidanceTaskException {
        TreeMap<String, ArrayList<String>> resultTreeMap = new TreeMap<>();

        try (BufferedReader br = (isFileGz)
                ? new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(this.filePath))))
                : new BufferedReader(new FileReader(this.filePath))) {
            // Variable to store the indexes
            List<Integer> indexes = null;

            // Read the header
            String header = br.readLine();
            if (header != null && !header.isEmpty()) {
                Hashtable<String, Integer> hashTableIndex = HashUtils.createHashWithHeader(header, " ");
                indexes = constructIndexes(hashTableIndex);
            }

            String line = null;
            while ((line = br.readLine()) != null) {
                processLine(line, indexes, resultTreeMap);
            }
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot read input file", ioe);
        }

        return resultTreeMap;
    }

    public List<Integer> constructIndexes(Hashtable<String, Integer> hashTableIndex) {
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

    public void processLine(String line, List<Integer> indexes, TreeMap<String, ArrayList<String>> resultTreeMap) {
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
