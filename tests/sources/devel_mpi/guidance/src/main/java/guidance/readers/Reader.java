package guidance.readers;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import guidance.exceptions.GuidanceTaskException;
import guidance.utils.HashUtils;


public abstract class Reader {

    private final String filePath;
    private final boolean isFileGz;


    public Reader(String filePath, boolean isFileGz) {
        this.filePath = filePath;
        this.isFileGz = isFileGz;
    }

    public TreeMap<String, ArrayList<String>> read(String separator, Object... extraParams) throws GuidanceTaskException {
        TreeMap<String, ArrayList<String>> resultTreeMap = new TreeMap<>();

        try (BufferedReader br = (isFileGz)
                ? new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(this.filePath))))
                : new BufferedReader(new FileReader(this.filePath))) {
            // Variable to store the indexes
            List<Integer> indexes = null;

            // Read the header
            String header = br.readLine();
            if (header != null && !header.isEmpty()) {
                Hashtable<String, Integer> hashTableIndex = HashUtils.createHashWithHeader(header, separator);
                indexes = constructIndexes(header, hashTableIndex, extraParams);
            }

            String line = null;
            while ((line = br.readLine()) != null) {
                processLine(line, indexes, resultTreeMap, extraParams);
            }
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot read input file", ioe);
        }

        return resultTreeMap;
    }

    protected abstract List<Integer> constructIndexes(String header, Hashtable<String, Integer> hashTableIndex, Object... extraParams);

    protected abstract void processLine(String line, List<Integer> indexes, TreeMap<String, ArrayList<String>> resultTreeMap,
            Object... extraParams);

}
