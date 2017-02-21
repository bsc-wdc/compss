package guidance.readers;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;


public class FilteredByAllXReader extends Reader {

    public FilteredByAllXReader(String filePath, boolean isFileGz) {
        super(filePath, isFileGz);
    }

    @Override
    protected List<Integer> constructIndexes(String header, Hashtable<String, Integer> hashTableIndex, Object... extraParams) {
        int indexChrInFilteredX = hashTableIndex.get("chr");
        int indexPositionInFilteredX = hashTableIndex.get("position");
        int indexRsIdInFilteredX = hashTableIndex.get("rs_id_all");
        int indexAlleleAInFilteredX = hashTableIndex.get("alleleA");
        int indexAlleleBInFilteredX = hashTableIndex.get("alleleB");
        int indexAllMafInFilteredX = hashTableIndex.get("all_maf");
        int indexFreqAddPvalueInFilteredX = hashTableIndex.get("frequentist_add_pvalue");
        // int indexFreqAddBetaInFilteredX = filteredByAllXHashTableIndex.get("frequentist_add_beta_1");
        // int indexFreqAddSeInFilteredX = filteredByAllXHashTableIndex.get("frequentist_add_se_1");
        int indexFreqAddBeta1sex1InFilteredX = hashTableIndex.get("frequentist_add_beta_1:genotype/sex=1");
        int indexFreqAddBeta2sex2InFilteredX = hashTableIndex.get("frequentist_add_beta_2:genotype/sex=2");
        int indexFreqAddSe1sex1InFilteredX = hashTableIndex.get("frequentist_add_se_1:genotype/sex=1");
        int indexFreqAddSe2sex2InFilteredX = hashTableIndex.get("frequentist_add_se_2:genotype/sex=2");

        List<Integer> indexes = new LinkedList<Integer>();
        indexes.add(indexChrInFilteredX);
        indexes.add(indexPositionInFilteredX);
        indexes.add(indexRsIdInFilteredX);
        indexes.add(indexAlleleAInFilteredX);
        indexes.add(indexAlleleBInFilteredX);
        indexes.add(indexAllMafInFilteredX);
        indexes.add(indexFreqAddPvalueInFilteredX);
        // indexes.add(indexFreqAddBetaInFilteredX);
        // indexes.add(indexFreqAddSeInFilteredX);
        indexes.add(indexFreqAddBeta1sex1InFilteredX);
        indexes.add(indexFreqAddBeta2sex2InFilteredX);
        indexes.add(indexFreqAddSe1sex1InFilteredX);
        indexes.add(indexFreqAddSe2sex2InFilteredX);

        return indexes;
    }

    @Override
    protected void processLine(String line, List<Integer> indexes, TreeMap<String, ArrayList<String>> resultTreeMap,
            Object... extraParams) {
        
        int indexChrInFilteredX = indexes.get(0);
        int indexPositionInFilteredX = indexes.get(1);
        int indexRsIdInFilteredX = indexes.get(2);
        int indexAlleleAInFilteredX = indexes.get(3);
        int indexAlleleBInFilteredX = indexes.get(4);
        int indexAllMafInFilteredX = indexes.get(5);
        int indexFreqAddPvalueInFilteredX = indexes.get(6);
        // int indexFreqAddBetaInFilteredX = indexes.get(0);
        // int indexFreqAddSeInFilteredX = indexes.get(0);
        int indexFreqAddBeta1sex1InFilteredX = indexes.get(7);
        int indexFreqAddBeta2sex2InFilteredX = indexes.get(8);
        int indexFreqAddSe1sex1InFilteredX = indexes.get(9);
        int indexFreqAddSe2sex2InFilteredX = indexes.get(10);

        String[] splitted = line.split("\t");
        String chrAndPosition = splitted[indexChrInFilteredX] + "_" + splitted[indexPositionInFilteredX];

        ArrayList<String> reducedList = new ArrayList<String>();
        reducedList.add(splitted[indexRsIdInFilteredX]);
        reducedList.add(splitted[indexAlleleAInFilteredX]);
        reducedList.add(splitted[indexAlleleBInFilteredX]);
        reducedList.add(splitted[indexAllMafInFilteredX]);
        reducedList.add(splitted[indexFreqAddPvalueInFilteredX]);

        // This to values for chr23
        reducedList.add("NA");
        reducedList.add("NA");

        // Now we put 4 values more that are the ones for chrX
        reducedList.add(splitted[indexFreqAddBeta1sex1InFilteredX]);
        reducedList.add(splitted[indexFreqAddBeta2sex2InFilteredX]);
        reducedList.add(splitted[indexFreqAddSe1sex1InFilteredX]);
        reducedList.add(splitted[indexFreqAddSe2sex2InFilteredX]);

        resultTreeMap.put(chrAndPosition, reducedList);
    }

}
