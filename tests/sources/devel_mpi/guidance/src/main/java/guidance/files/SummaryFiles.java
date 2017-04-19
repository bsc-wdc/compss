package guidance.files;

import java.util.List;

import guidance.utils.ChromoInfo;
import guidance.utils.ParseCmdLine;

import java.io.File;
import java.util.ArrayList;


/**
 * Class for the chromosomes
 *
 */
public class SummaryFiles {

    // Two dimensional array for the name of files for chunks:
    // Take into account that the maximum number of chunks is 84
    private ArrayList<ArrayList<String>> outDir = new ArrayList<>();

    private ArrayList<ArrayList<ArrayList<String>>> mergedGenFileName = new ArrayList<>();
    private ArrayList<ArrayList<ArrayList<String>>> mergedGenFile = new ArrayList<>();
    private ArrayList<ArrayList<ArrayList<String>>> mergedSampleFileName = new ArrayList<>();
    private ArrayList<ArrayList<ArrayList<String>>> mergedSampleFile = new ArrayList<>();
    private ArrayList<ArrayList<ArrayList<String>>> mergedLogFileName = new ArrayList<>();
    private ArrayList<ArrayList<ArrayList<String>>> mergedLogFile = new ArrayList<>();

    private ArrayList<ArrayList<ArrayList<String>>> snptestOutFileName = new ArrayList<>();
    private ArrayList<ArrayList<ArrayList<String>>> snptestOutFile = new ArrayList<>();
    private ArrayList<ArrayList<ArrayList<String>>> snptestLogFileName = new ArrayList<>();
    private ArrayList<ArrayList<ArrayList<String>>> snptestLogFile = new ArrayList<>();

    private ArrayList<ArrayList<ArrayList<String>>> summaryFileName = new ArrayList<>();
    private ArrayList<ArrayList<ArrayList<String>>> summaryFile = new ArrayList<>();

    private ArrayList<ArrayList<ArrayList<String>>> reducedListFileName = new ArrayList<>();
    private ArrayList<ArrayList<ArrayList<String>>> reducedListFile = new ArrayList<>();

    private ArrayList<ArrayList<String>> filteredByAllFileName = new ArrayList<>();
    private ArrayList<ArrayList<String>> filteredByAllFile = new ArrayList<>();


    /**
     * A first constructor used when typeGWAS is "separated"
     * 
     * @param parsingArgs
     * @param generalChromoInfo
     * @param myOutDir
     * @param refPanels
     */
    public SummaryFiles(ParseCmdLine parsingArgs, ChromoInfo generalChromoInfo, String myOutDir, List<String> refPanels) {
        int chunkSize = parsingArgs.getChunkSize();

        String testTypeName = parsingArgs.getTestTypeName(0);

        // We create the first directory name: the cohort directory
        String mixedCohort = parsingArgs.getCohort();

        for (int j = 0; j < refPanels.size(); j++) {
            String rPanel = refPanels.get(j);
            // System.out.println("[SummaryFiles]: For refPanel " + rPanel);

            String assocOutDir = myOutDir + File.separator + "associations" + File.separator + testTypeName + File.separator + mixedCohort
                    + "_for_" + rPanel;

            ArrayList<String> chromoListOutDir = new ArrayList<>();

            ArrayList<ArrayList<String>> chromoListMergedGenFileName = new ArrayList<>();
            ArrayList<ArrayList<String>> chromoListMergedGenFile = new ArrayList<>();
            ArrayList<ArrayList<String>> chromoListMergedSampleFileName = new ArrayList<>();
            ArrayList<ArrayList<String>> chromoListMergedSampleFile = new ArrayList<>();
            ArrayList<ArrayList<String>> chromoListMergedLogFileName = new ArrayList<>();
            ArrayList<ArrayList<String>> chromoListMergedLogFile = new ArrayList<>();

            ArrayList<ArrayList<String>> chromoListSnptestOutFileName = new ArrayList<>();
            ArrayList<ArrayList<String>> chromoListSnptestOutFile = new ArrayList<>();
            ArrayList<ArrayList<String>> chromoListSnptestLogFileName = new ArrayList<>();
            ArrayList<ArrayList<String>> chromoListSnptestLogFile = new ArrayList<>();
            ArrayList<ArrayList<String>> chromoListSummaryFileName = new ArrayList<>();
            ArrayList<ArrayList<String>> chromoListSummaryFile = new ArrayList<>();

            ArrayList<ArrayList<String>> chromoListReducedFileName = new ArrayList<>();
            ArrayList<ArrayList<String>> chromoListReducedFile = new ArrayList<>();

            ArrayList<String> chromoFilteredByAllFileName = new ArrayList<>();
            ArrayList<String> chromoFilteredByAllFile = new ArrayList<>();

            for (int chromo = 1; chromo <= ChromoInfo.MAX_NUMBER_OF_CHROMOSOMES; ++chromo) {
                String tmpChrDir = assocOutDir + File.separator + "Chr_" + chromo;
                chromoListOutDir.add(tmpChrDir);

                int maxSize = generalChromoInfo.getMaxSize(chromo);
                int total_chunks = maxSize / chunkSize;
                int module = maxSize % chunkSize;
                if (module != 0) {
                    total_chunks++;
                }

                int lim1 = 1;
                int lim2 = lim1 + chunkSize - 1;

                ArrayList<String> chunkListMergedGenFileName = new ArrayList<>();
                ArrayList<String> chunkListMergedGenFile = new ArrayList<>();
                ArrayList<String> chunkListMergedSampleFileName = new ArrayList<>();
                ArrayList<String> chunkListMergedSampleFile = new ArrayList<>();
                ArrayList<String> chunkListMergedLogFileName = new ArrayList<>();
                ArrayList<String> chunkListMergedLogFile = new ArrayList<>();

                ArrayList<String> chunkListSnptestOutFileName = new ArrayList<>();
                ArrayList<String> chunkListSnptestOutFile = new ArrayList<>();
                ArrayList<String> chunkListSnptestLogFileName = new ArrayList<>();
                ArrayList<String> chunkListSnptestLogFile = new ArrayList<>();
                ArrayList<String> chunkListSummaryFileName = new ArrayList<>();
                ArrayList<String> chunkListSummaryFile = new ArrayList<>();

                ArrayList<String> listReducedFileName = new ArrayList<>();
                ArrayList<String> listReducedFile = new ArrayList<>();

                for (int k = 0; k < total_chunks; k++) {
                    // Now we have to create the impute files for this iteration for merged
                    String tmpGenFileName = "chr_" + chromo + "_" + lim1 + "_" + lim2 + "_merged.gen";
                    chunkListMergedGenFileName.add(tmpGenFileName);
                    String tmpGenFile = tmpChrDir + File.separator + tmpGenFileName;
                    chunkListMergedGenFile.add(tmpGenFile);

                    String tmpSampleFileName = "chr_" + chromo + "_" + lim1 + "_" + lim2 + "_merged.sample";
                    chunkListMergedSampleFileName.add(tmpSampleFileName);
                    String tmpSampleFile = tmpChrDir + File.separator + tmpSampleFileName;
                    chunkListMergedSampleFile.add(tmpSampleFile);

                    String tmpLogFileName = "chr_" + chromo + "_" + lim1 + "_" + lim2 + "_merged.log";
                    chunkListMergedLogFileName.add(tmpLogFileName);
                    String tmpLogFile = tmpChrDir + File.separator + tmpLogFileName;
                    chunkListMergedLogFile.add(tmpLogFile);

                    String tmpSnptestOutFileName = "chr_" + chromo + "_" + lim1 + "_" + lim2 + "_snptest.out";
                    chunkListSnptestOutFileName.add(tmpSnptestOutFileName);
                    String tmpSnptestOutFile = tmpChrDir + File.separator + tmpSnptestOutFileName;
                    chunkListSnptestOutFile.add(tmpSnptestOutFile);

                    String tmpSnptestLogFileName = "chr_" + chromo + "_" + lim1 + "_" + lim2 + "_snptest.log";
                    chunkListSnptestLogFileName.add(tmpSnptestLogFileName);
                    String tmpSnptestLogFile = tmpChrDir + File.separator + tmpSnptestLogFileName;
                    chunkListSnptestLogFile.add(tmpSnptestLogFile);

                    String tmpSummaryFileName = "chr_" + chromo + "_" + lim1 + "_" + lim2 + "_summary.txt";
                    chunkListSummaryFileName.add(tmpSummaryFileName);
                    String tmpSummaryFile = tmpChrDir + File.separator + tmpSummaryFileName;
                    chunkListSummaryFile.add(tmpSummaryFile);

                    listReducedFileName.add(tmpSummaryFileName);
                    listReducedFile.add(tmpSummaryFile);

                    lim1 = lim1 + chunkSize;
                    lim2 = lim2 + chunkSize;
                }
                int index1 = 0;
                int index2 = 1;
                int current_index = listReducedFileName.size();
                // System.out.println("\t[SummaryFiles]: index1 " + index1 + " |index2 " + index2 + " |current_index " +
                // current_index);

                int counter = 0;
                while (index2 < current_index) {
                    String tmpReducedFileName = "chr_" + chromo + "_reduce_file_" + counter + ".txt";
                    String tmpReducedFile = tmpChrDir + File.separator + tmpReducedFileName;

                    // System.out.println("\t[SummaryFiles]: " + tmpReducedFileName);
                    // System.out.println("\t[SummaryFiles]: " + tmpReducedFile);

                    listReducedFileName.add(tmpReducedFileName);
                    listReducedFile.add(tmpReducedFile);
                    index1 = index1 + 2;
                    index2 = index2 + 2;
                    current_index++;
                    counter++;
                }

                chromoListMergedGenFileName.add(chunkListMergedGenFileName);
                chromoListMergedGenFile.add(chunkListMergedGenFile);
                chromoListMergedSampleFileName.add(chunkListMergedSampleFileName);
                chromoListMergedSampleFile.add(chunkListMergedSampleFile);
                chromoListMergedLogFileName.add(chunkListMergedLogFileName);
                chromoListMergedLogFile.add(chunkListMergedLogFile);
                chromoListSnptestOutFileName.add(chunkListSnptestOutFileName);
                chromoListSnptestOutFile.add(chunkListSnptestOutFile);
                chromoListSnptestLogFileName.add(chunkListSnptestLogFileName);
                chromoListSnptestLogFile.add(chunkListSnptestLogFile);
                chromoListSummaryFileName.add(chunkListSummaryFileName);
                chromoListSummaryFile.add(chunkListSummaryFile);

                chromoListReducedFileName.add(listReducedFileName);
                chromoListReducedFile.add(listReducedFile);

                String tmpFilteredByAllFileName = "chr_" + chromo + "_filtered_by_maf_info_hwe.txt";
                String tmpFilteredByAllFile = tmpChrDir + File.separator + tmpFilteredByAllFileName;

                chromoFilteredByAllFileName.add(tmpFilteredByAllFileName);
                chromoFilteredByAllFile.add(tmpFilteredByAllFile);
            }

            outDir.add(chromoListOutDir);

            mergedGenFileName.add(chromoListMergedGenFileName);
            mergedGenFile.add(chromoListMergedGenFile);
            mergedSampleFileName.add(chromoListMergedSampleFileName);
            mergedSampleFile.add(chromoListMergedSampleFile);
            mergedLogFileName.add(chromoListMergedLogFileName);
            mergedLogFile.add(chromoListMergedLogFile);
            snptestOutFileName.add(chromoListSnptestOutFileName);
            snptestOutFile.add(chromoListSnptestOutFile);
            snptestLogFileName.add(chromoListSnptestLogFileName);
            snptestLogFile.add(chromoListSnptestLogFile);
            summaryFileName.add(chromoListSummaryFileName);
            summaryFile.add(chromoListSummaryFile);
            reducedListFileName.add(chromoListReducedFileName);
            reducedListFile.add(chromoListReducedFile);

            filteredByAllFileName.add(chromoFilteredByAllFileName);
            filteredByAllFile.add(chromoFilteredByAllFile);

        }
        // System.out.println("\t[SummaryFiles]: OK done for SummaryFiles");
    }

    /**
     * Method to access mergedGenDir information
     * 
     * @param rPanelIndex
     * @param chromo
     * @return
     */
    public String getAssocOutDir(int rPanelIndex, int chromo) {
        validateChromo(chromo, 1, 1);

        int i = chromo - 1;

        return outDir.get(rPanelIndex).get(i);
    }

    /**
     * Method to access mergedGenFileName
     * 
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getMergedGenFileName(int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        int i = chromo - 1;
        int index = lim1 / chunkSize;

        return mergedGenFileName.get(rPanelIndex).get(i).get(index);
    }

    /**
     * Method to access mergedGenFile
     * 
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getMergedGenFile(int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        int i = chromo - 1;
        int index = lim1 / chunkSize;

        return mergedGenFile.get(rPanelIndex).get(i).get(index);
    }

    /**
     * Method to access mergedSampleFileName
     * 
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getMergedSampleFileName(int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        int i = chromo - 1;
        int index = lim1 / chunkSize;

        return mergedSampleFileName.get(rPanelIndex).get(i).get(index);
    }

    /**
     * Method to access mergedSampleFile
     * 
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getMergedSampleFile(int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        int i = chromo - 1;
        int index = lim1 / chunkSize;

        return mergedSampleFile.get(rPanelIndex).get(i).get(index);
    }

    /**
     * Method to access mergedLogFileName
     * 
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getMergedLogFileName(int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        int i = chromo - 1;
        int index = lim1 / chunkSize;

        return mergedLogFileName.get(rPanelIndex).get(i).get(index);
    }

    /**
     * Method to access mergedLogFile
     * 
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getMergedLogFile(int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        int i = chromo - 1;
        int index = lim1 / chunkSize;

        return mergedLogFile.get(rPanelIndex).get(i).get(index);
    }

    /**
     * Method to access snptestOutFileName
     * 
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getSnptestOutFileName(int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        int i = chromo - 1;
        int index = lim1 / chunkSize;

        return snptestOutFileName.get(rPanelIndex).get(i).get(index);
    }

    /**
     * Method to access snptestOutFile
     * 
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getSnptestOutFile(int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        int i = chromo - 1;
        int index = lim1 / chunkSize;

        return snptestOutFile.get(rPanelIndex).get(i).get(index);
    }

    /**
     * Method to access snptestLogFileName
     * 
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getSnptestLogFileName(int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        int i = chromo - 1;
        int index = lim1 / chunkSize;

        return snptestLogFileName.get(rPanelIndex).get(i).get(index);
    }

    /**
     * Method to access snptestLogFile
     * 
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getSnptestLogFile(int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        int i = chromo - 1;
        int index = lim1 / chunkSize;

        return snptestLogFile.get(rPanelIndex).get(i).get(index);
    }

    /**
     * Method to access snptestLogFileName
     * 
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getSummaryFileName(int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        int i = chromo - 1;
        int index = lim1 / chunkSize;

        return summaryFileName.get(rPanelIndex).get(i).get(index);
    }

    /**
     * Method to access snptestLogFile
     * 
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getSummaryFile(int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        int i = chromo - 1;
        int index = lim1 / chunkSize;

        return summaryFile.get(rPanelIndex).get(i).get(index);
    }

    /**
     * Method to access reducedFileName
     * 
     * @param rPanelIndex
     * @param chromo
     * @param index
     * @return
     */
    public String getReducedFileName(int rPanelIndex, int chromo, int index) {
        // System.out.println("[SummaryFiles]: " + rPanelIndex + " |chromo " + chromo + " |index " + index);

        validateChromo(chromo, 1, 1);

        int i = chromo - 1;

        // TODO: Important, Verify the index range!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // reducedListFileName.get(rPanelIndex).get(i);
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[SummaryFiles] Error, the number of reducedListFileName is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return reducedListFile.get(rPanelIndex).get(i).get(index);
    }

    /**
     * Method to access reducedFile
     * 
     * @param rPanelIndex
     * @param chromo
     * @param index
     * @return
     */
    public String getReducedFile(int rPanelIndex, int chromo, int index) {
        validateChromo(chromo, 1, 1);

        int i = chromo - 1;

        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (reducedListFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[SummaryFiles] Error, the number of reducedListFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return reducedListFile.get(rPanelIndex).get(i).get(index);
    }

    /**
     * Method to access reducedFile
     * 
     * @param rPanelIndex
     * @param chromo
     * @return
     */
    public String getTheLastReducedFile(int rPanelIndex, int chromo) {
        validateChromo(chromo, 1, 1);

        int i = chromo - 1;

        // REMOVE ArrayList<String> tmpList = new ArrayList<String>();
        int lastIndex = reducedListFile.get(rPanelIndex).get(i).size() - 1;
        // System.out.println("[SummaryFiles] lastIndex size = " + lastIndex);

        return reducedListFile.get(rPanelIndex).get(i).get(lastIndex);
    }

    /**
     * Method to access filteredByAllFileName
     * 
     * @param rPanelIndex
     * @param chromo
     * @return
     */
    public String getFilteredByAllFileName(int rPanelIndex, int chromo) {
        validateChromo(chromo, 1, 1);

        int i = chromo - 1;

        return filteredByAllFileName.get(rPanelIndex).get(i);
    }

    /**
     * Method to access filteredByAllFile
     * 
     * @param rPanelIndex
     * @param chromo
     * @return
     */
    public String getFilteredByAllFile(int rPanelIndex, int chromo) {
        validateChromo(chromo, 1, 1);

        int i = chromo - 1;

        return filteredByAllFile.get(rPanelIndex).get(i);
    }

    /**
     * Method to print merged files information
     * 
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     */
    public void printSummaryFiles(int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        int i = chromo - 1;
        int indexLow = lim1 / chunkSize;
        int indexHigh = lim2 / chunkSize;

        for (int j = indexLow; j < indexHigh; j++) {
            System.out.println("-------------------------------------------------");
            System.out.println("Assoc files information for the chromosome " + chromo);
            System.out.println("outDir                  : " + outDir.get(rPanelIndex).get(i));
            System.out.println("mergedGenFileName       : " + mergedGenFileName.get(rPanelIndex).get(i).get(indexLow));
            System.out.println("mergedGenFile           : " + mergedGenFile.get(rPanelIndex).get(i).get(indexLow));
            System.out.println("mergedSampleFileName    : " + mergedSampleFileName.get(rPanelIndex).get(i).get(indexLow));
            System.out.println("mergedSampleFile        : " + mergedSampleFile.get(rPanelIndex).get(i).get(indexLow));
            System.out.println("mergedLogFileName       : " + mergedLogFileName.get(rPanelIndex).get(i).get(indexLow));
            System.out.println("mergedLogFile           : " + mergedLogFile.get(rPanelIndex).get(i).get(indexLow));

            System.out.println("mergedSnptestOutFileName: " + snptestOutFileName.get(rPanelIndex).get(i).get(indexLow));
            System.out.println("mergedSnptestOutFile    : " + snptestOutFile.get(rPanelIndex).get(i).get(indexLow));
            System.out.println("mergedSnptestLogFileName: " + snptestLogFileName.get(rPanelIndex).get(i).get(indexLow));
            System.out.println("mergedSnptestLogFile    : " + snptestLogFile.get(rPanelIndex).get(i).get(indexLow));
            System.out.println("-------------------------------------------------");
        }
    }

    /**
     * Private method to validate the chromo values received in all public methods
     * 
     * @param chromo
     * @param lim1
     * @param lim2
     */
    private void validateChromo(int chromo, int lim1, int lim2) {
        if (chromo < 1 || chromo > ChromoInfo.MAX_NUMBER_OF_CHROMOSOMES) {
            System.err.println("[SummaryFiles] Error, chromosomesome " + chromo + " does not exist");
            System.exit(1);
        }

        if (lim1 < 1 || lim2 > ChromoInfo.MAX_CHROMOSOME_LIMIT) {
            System.err.println("[SummaryFiles] Error, Chunk " + lim1 + "_" + lim2 + " does not exist for chromosome " + chromo);
            System.exit(1);
        }
    }

}
