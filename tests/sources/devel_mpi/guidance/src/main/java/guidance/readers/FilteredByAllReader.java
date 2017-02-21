package guidance.readers;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;


public class FilteredByAllReader extends Reader {

    public FilteredByAllReader(String filePath, boolean isFileGz) {
        super(filePath, isFileGz);
    }

    @Override
    protected List<Integer> constructIndexes(String header, Hashtable<String, Integer> hashTableIndex, Object... extraParams) {
        int indexChrInFiltered = hashTableIndex.get("chr");
        int indexPositionInFiltered = hashTableIndex.get("position");
        int indexRsIdInFiltered = hashTableIndex.get("rs_id_all");
        int indexAlleleAInFiltered = hashTableIndex.get("alleleA");
        int indexAlleleBInFiltered = hashTableIndex.get("alleleB");
        int indexAllMafInFiltered = hashTableIndex.get("all_maf");
        int indexFreqAddPvalueInFiltered = hashTableIndex.get("frequentist_add_pvalue");
        int indexFreqAddBetaInFiltered = hashTableIndex.get("frequentist_add_beta_1");
        // int indexFreqAddBeta1sex1InFiltered = hashTableIndex.get("frequentist_add_beta_1:genotype/sex=1");
        // int indexFreqAddBeta2sex2InFiltered = hashTableIndex.get("frequentist_add_beta_2:genotype/sex=2");
        int indexFreqAddSeInFiltered = hashTableIndex.get("frequentist_add_se_1");
        // int indexFreqAddSe1sex1InFiltered = hashTableIndex.get("frequentist_add_se_1:genotype/sex=1");
        // int indexFreqAddSe2sex2InFiltered = hashTableIndex.get("frequentist_add_se_2:genotype/sex=2");

        List<Integer> indexes = new LinkedList<Integer>();
        indexes.add(indexChrInFiltered);
        indexes.add(indexPositionInFiltered);
        indexes.add(indexRsIdInFiltered);
        indexes.add(indexAlleleAInFiltered);
        indexes.add(indexAlleleBInFiltered);
        indexes.add(indexAllMafInFiltered);
        indexes.add(indexFreqAddPvalueInFiltered);
        indexes.add(indexFreqAddBetaInFiltered);
        // indexes.add(indexFreqAddBeta1sex1InFiltered);
        // indexes.add(indexFreqAddBeta1sex1InFiltered);
        indexes.add(indexFreqAddSeInFiltered);
        // indexes.add(indexFreqAddSe1sex1InFiltered);
        // indexes.add(indexFreqAddSe2sex2InFiltered);

        return indexes;
    }

    @Override
    protected void processLine(String line, List<Integer> indexes, TreeMap<String, ArrayList<String>> resultTreeMap,
            Object... extraParams) {
        
        int indexChrInFiltered = indexes.get(0);
        int indexPositionInFiltered = indexes.get(1);
        int indexRsIdInFiltered = indexes.get(2);
        int indexAlleleAInFiltered = indexes.get(3);
        int indexAlleleBInFiltered = indexes.get(4);
        int indexAllMafInFiltered = indexes.get(5);
        int indexFreqAddPvalueInFiltered = indexes.get(6);
        int indexFreqAddBetaInFiltered = indexes.get(7);
        // int indexFreqAddBeta1sex1InFiltered = indexes.get(1);
        // int indexFreqAddBeta1sex1InFiltered = indexes.get(1);
        int indexFreqAddSeInFiltered = indexes.get(8);
        // int indexFreqAddSe1sex1InFiltered = indexes.get(1);
        // int indexFreqAddSe2sex2InFiltered = indexes.get(1);

        String[] splitted = line.split("\t");
        String chrAndPosition = splitted[indexChrInFiltered] + "_" + splitted[indexPositionInFiltered];

        ArrayList<String> reducedList = new ArrayList<String>();
        reducedList.add(splitted[indexRsIdInFiltered]);
        reducedList.add(splitted[indexAlleleAInFiltered]);
        reducedList.add(splitted[indexAlleleBInFiltered]);
        reducedList.add(splitted[indexAllMafInFiltered]);
        reducedList.add(splitted[indexFreqAddPvalueInFiltered]);
        reducedList.add(splitted[indexFreqAddBetaInFiltered]);
        reducedList.add(splitted[indexFreqAddSeInFiltered]);

        // Now we put 4 values more that are the ones for chrX
        reducedList.add("NA");
        reducedList.add("NA");
        reducedList.add("NA");
        reducedList.add("NA");

        resultTreeMap.put(chrAndPosition, reducedList);
    }

}
