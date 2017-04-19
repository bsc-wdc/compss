package guidance.files;

import java.util.List;

import guidance.utils.ChromoInfo;
import guidance.utils.GenericFile;
import guidance.utils.ParseCmdLine;

import java.io.File;
import java.util.ArrayList;


public class MergeFiles {

    private ArrayList<ArrayList<ArrayList<String>>> testTypeReducedOutDir = new ArrayList<>();

    private ArrayList<ArrayList<ArrayList<ArrayList<GenericFile>>>> testTypeReducedFile = new ArrayList<>();

    private ArrayList<ArrayList<ArrayList<GenericFile>>> testTypeFilteredByAllFile = new ArrayList<>();
    private ArrayList<ArrayList<ArrayList<GenericFile>>> testTypeCondensedFile = new ArrayList<>();

    private ArrayList<ArrayList<ArrayList<GenericFile>>> testTypeAdditionalCondensedFile = new ArrayList<>();
    private ArrayList<ArrayList<ArrayList<GenericFile>>> testTypeAdditionalFilteredByAllFile = new ArrayList<>();
    private ArrayList<ArrayList<ArrayList<GenericFile>>> testTypeAdditionalFilteredByAllXFile = new ArrayList<>();

    private ArrayList<ArrayList<Integer>> testTypeAdditionalCondensedIndex = new ArrayList<>();
    private ArrayList<ArrayList<Integer>> testTypeAdditionalFilteredByAllIndex = new ArrayList<>();

    private int startChr = 0;
    private int endChr = 0;


    /**
     * Constructor for mergeFiles
     * 
     * @param parsingArgs
     * @param generalChromoInfo
     * @param baseOutDir
     * @param refPanels
     */
    public MergeFiles(ParseCmdLine parsingArgs, ChromoInfo generalChromoInfo, String baseOutDir, List<String> refPanels) {
        startChr = parsingArgs.getStart();
        endChr = parsingArgs.getEnd();

        int chunkSize = parsingArgs.getChunkSize();
        int numberOfTestTypesNames = parsingArgs.getNumberOfTestTypeName();

        // We create the first directory name: the cohort directory
        String mixedCohort = parsingArgs.getCohort();

        for (int tt = 0; tt < numberOfTestTypesNames; tt++) {
            String testTypeName = parsingArgs.getTestTypeName(tt);
            String testTypeOutDir = baseOutDir + File.separator + "associations" + File.separator + testTypeName;

            ArrayList<ArrayList<String>> rpanelListOutDir = new ArrayList<>();
            ArrayList<ArrayList<ArrayList<GenericFile>>> rpanelReducedFile = new ArrayList<>();

            ArrayList<ArrayList<GenericFile>> rpanelFilteredByAllFile = new ArrayList<>();
            ArrayList<ArrayList<GenericFile>> rpanelCondensedFile = new ArrayList<>();

            ArrayList<ArrayList<GenericFile>> rpanelAdditionalCondensedFile = new ArrayList<>();
            ArrayList<ArrayList<GenericFile>> rpanelAdditionalFilteredByAllFile = new ArrayList<>();
            ArrayList<ArrayList<GenericFile>> rpanelAdditionalFilteredByAllXFile = new ArrayList<>();

            ArrayList<Integer> rpanelAdditionalCondensedIndex = new ArrayList<>();
            ArrayList<Integer> rpanelAdditionalFilteredByAllIndex = new ArrayList<>();

            for (int j = 0; j < refPanels.size(); j++) {
                String rPanel = refPanels.get(j);
                String rpanelOutDir = testTypeOutDir + File.separator + mixedCohort + "_for_" + rPanel;
                String rpanelOutDirSummary = testTypeOutDir + File.separator + mixedCohort + "_for_" + rPanel + File.separator + "summary";

                ArrayList<String> chromoListOutDir = new ArrayList<>();
                ArrayList<ArrayList<GenericFile>> chromoListReducedFile = new ArrayList<>();
                ArrayList<GenericFile> chromoFilteredByAllFile = new ArrayList<>();
                ArrayList<GenericFile> chromoCondensedFile = new ArrayList<>();

                ArrayList<GenericFile> additionalCondensedFile = new ArrayList<>();
                ArrayList<GenericFile> additionalFilteredByAllFile = new ArrayList<>();
                ArrayList<GenericFile> additionalFilteredByAllXFile = new ArrayList<>();

                for (int chromo = startChr; chromo <= endChr; ++chromo) {
                    String tmpChrDir = rpanelOutDir + File.separator + "Chr_" + chromo;
                    chromoListOutDir.add(tmpChrDir);

                    int maxSize = generalChromoInfo.getMaxSize(chromo);
                    int total_chunks = maxSize / chunkSize;
                    int module = maxSize % chunkSize;
                    if (module != 0) {
                        total_chunks++;
                    }

                    ArrayList<GenericFile> listReducedFile = new ArrayList<>();

                    // Now we have to create the rest of file names that will be used to reduce files
                    int counter = 0;

                    for (int deep = 0; deep < total_chunks - 1; deep++) {
                        String tmpReducedFileName = "chr_" + chromo + "_" + testTypeName + "_" + rPanel + "_reduce_file_" + counter
                                + ".txt.gz";
                        // String tmpReducedFile = tmpChrDir + File.separator + tmpReducedFileName;

                        GenericFile myReducedFile = new GenericFile(tmpChrDir, tmpReducedFileName, "uncompressed", "none");
                        listReducedFile.add(myReducedFile);
                        counter++;
                    }

                    // Now we have built the list of reduce file for this chromosome. We store this list
                    chromoListReducedFile.add(listReducedFile);

                    String tmpFilteredByAllFileName = "chr_" + chromo + "_" + testTypeName + "_" + rPanel
                            + "_filtered_by_maf_info_hwe.txt.gz";
                    // String tmpFilteredByAllFile = tmpChrDir + File.separator + tmpFilteredByAllFileName;
                    GenericFile myFilteredByAllFile = new GenericFile(tmpChrDir, tmpFilteredByAllFileName, "uncompressed", "none");
                    chromoFilteredByAllFile.add(myFilteredByAllFile);

                    String tmpCondensedFileName = "chr_" + chromo + "_" + testTypeName + "_" + rPanel + "_condensed.txt.gz";
                    // String tmpCondensedFile = tmpChrDir + File.separator + tmpCondensedFileName;
                    GenericFile myCondensedFile = new GenericFile(tmpChrDir, tmpCondensedFileName, "uncompressed", "none");
                    chromoCondensedFile.add(myCondensedFile);

                } // End of for chromosomes
                rpanelListOutDir.add(chromoListOutDir);
                rpanelReducedFile.add(chromoListReducedFile);

                rpanelFilteredByAllFile.add(chromoFilteredByAllFile);
                rpanelCondensedFile.add(chromoCondensedFile);

                // Here we have to create an additional list of condensed files that will be used when we execute
                // jointCondensedFiles Task,
                // for all chromosomes.
                // The number of additional files is the number of chromosomes minus 1.
                int addCondensed = 0;
                for (int deep = startChr; deep < endChr; deep++) {
                    String tmpAdditionalCondensedFileName = null;
                    // if(startChr == endChr) {
                    // tmpAdditionalCondensedFileName = testTypeName + "_" + rPanel + "_condensed_chr_" + startChr +
                    // ".txt.gz";
                    // } else
                    if (deep == (endChr - 1)) {
                        tmpAdditionalCondensedFileName = testTypeName + "_" + rPanel + "_condensed_chr_" + startChr + "_to_" + endChr
                                + ".txt.gz";
                    } else {
                        tmpAdditionalCondensedFileName = testTypeName + "_" + rPanel + "_condensed_" + addCondensed + ".txt.gz";
                    }
                    // String tmpAdditionalCondensedFile = rpanelOutDirSummary + File.separator +
                    // tmpAdditionalCondensedFileName;
                    GenericFile myAdditionalCondensedFile = new GenericFile(rpanelOutDirSummary, tmpAdditionalCondensedFileName,
                            "uncompressed", "none");
                    additionalCondensedFile.add(myAdditionalCondensedFile);
                    // System.out.println("\t[MergeFiles.java] " + tmpAdditionalCondensedFile);

                    addCondensed++;
                }

                if (startChr == endChr) {
                    String tmpAdditionalCondensedFileName = testTypeName + "_" + rPanel + "_condensed_chr_" + startChr + ".txt.gz";
                    // String tmpAdditionalCondensedFile = rpanelOutDirSummary + File.separator +
                    // tmpAdditionalCondensedFileName;
                    GenericFile myAdditionalCondensedFile = new GenericFile(rpanelOutDirSummary, tmpAdditionalCondensedFileName,
                            "uncompressed", "none");
                    additionalCondensedFile.add(myAdditionalCondensedFile);
                    // System.out.println("\t[MergeFiles.java] only " + tmpAdditionalCondensedFile);
                    addCondensed++;
                }

                rpanelAdditionalCondensedIndex.add(addCondensed);

                // Here we have to create an additional list of filteredByAll files that will be used when we execute
                // jointFilteredByAllFile task
                // for all chromosomes.
                // Unlike the previous case with condensed files, we can not include chromosome 23. (Chr 23 format fo
                // filteredByAllFile is different to
                // the rest of chromosomes (thanks to snptest).

                // The number of additional files is the number of chromosomes minus 1.
                int addFiltered = 0;
                int endChrNormal = endChr;
                if (startChr < endChr) {
                    if (endChr != ChromoInfo.MAX_NUMBER_OF_CHROMOSOMES) {
                        endChrNormal = endChr;
                    } else {
                        endChrNormal = endChr - 1;
                        // Aqui deberiamos hacer el chr23?
                    }
                }

                for (int deep = startChr; deep < endChrNormal; deep++) {
                    String tmpAdditionalFilteredByAllFileName = null;
                    // if(startChr == endChrNormal) {
                    // tmpAdditionalFilteredByAllFileName = testTypeName + "_" + rPanel + "_filteredByAll_chr_" +
                    // startChr + ".txt.gz";
                    // } else
                    if (deep == (endChrNormal - 1)) {
                        tmpAdditionalFilteredByAllFileName = testTypeName + "_" + rPanel + "_filteredByAll_chr_" + startChr + "_to_"
                                + endChrNormal + ".txt.gz";
                    } else {
                        tmpAdditionalFilteredByAllFileName = testTypeName + "_" + rPanel + "_filteredByAll_" + addFiltered + ".txt.gz";
                    }
                    // String tmpAdditionalFilteredByAllFile = rpanelOutDirSummary + File.separator +
                    // tmpAdditionalFilteredByAllFileName;
                    GenericFile myAdditionalFilteredByAllFile = new GenericFile(rpanelOutDirSummary, tmpAdditionalFilteredByAllFileName,
                            "uncompressed", "none");
                    additionalFilteredByAllFile.add(myAdditionalFilteredByAllFile);
                    // System.out.println("\t[MergeFiles.java] " + tmpAdditionalFilteredByAllFile);

                    addFiltered++;
                }

                if (startChr == endChrNormal) {
                    String tmpAdditionalFilteredByAllFileName = testTypeName + "_" + rPanel + "_filteredByAll_chr_" + startChr + ".txt.gz";
                    // String tmpAdditionalFilteredByAllFile = rpanelOutDirSummary + File.separator +
                    // tmpAdditionalFilteredByAllFileName;
                    GenericFile myAdditionalFilteredByAllFile = new GenericFile(rpanelOutDirSummary, tmpAdditionalFilteredByAllFileName,
                            "uncompressed", "none");
                    additionalFilteredByAllFile.add(myAdditionalFilteredByAllFile);
                    // System.out.println("\t[MergeFiles.java] only " + tmpAdditionalFilteredByAllFile);
                    addFiltered++;
                }

                rpanelAdditionalFilteredByAllIndex.add(addFiltered);

                rpanelAdditionalCondensedFile.add(additionalCondensedFile);
                rpanelAdditionalFilteredByAllFile.add(additionalFilteredByAllFile);

                // If there is chr 23:
                if (endChr == ChromoInfo.MAX_NUMBER_OF_CHROMOSOMES) {
                    String tmpAdditionalFilteredByAllXFileName = testTypeName + "_" + rPanel + "_filteredByAll_chr_" + endChr + ".txt.gz";
                    // String tmpAdditionalFilteredByAllXFile = rpanelOutDirSummary + File.separator +
                    // tmpAdditionalFilteredByAllXFileName;
                    GenericFile myAdditionalFilteredByAllXFile = new GenericFile(rpanelOutDirSummary, tmpAdditionalFilteredByAllXFileName,
                            "uncompressed", "none");

                    additionalFilteredByAllXFile.add(myAdditionalFilteredByAllXFile);
                    // System.out.println("\t[MergeFiles.java] " + tmpAdditionalFilteredByAllXFile);

                    rpanelAdditionalFilteredByAllXFile.add(additionalFilteredByAllXFile);
                }
            } // End of for panels

            // Now we have to build the list of reduced files for the type of Test. We store this list
            testTypeReducedOutDir.add(rpanelListOutDir);
            testTypeReducedFile.add(rpanelReducedFile);

            testTypeFilteredByAllFile.add(rpanelFilteredByAllFile);
            testTypeCondensedFile.add(rpanelCondensedFile);

            testTypeAdditionalCondensedFile.add(rpanelAdditionalCondensedFile);
            testTypeAdditionalFilteredByAllFile.add(rpanelAdditionalFilteredByAllFile);

            if (endChr == 23) {
                testTypeAdditionalFilteredByAllXFile.add(rpanelAdditionalFilteredByAllXFile);
            }

            testTypeAdditionalCondensedIndex.add(rpanelAdditionalCondensedIndex);
            testTypeAdditionalFilteredByAllIndex.add(rpanelAdditionalFilteredByAllIndex);

        } // End of for test types
    }

    /**
     * Method to access mergedGenDir information
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @return
     */
    public String getAssocOutDir(int testTypeIndex, int rPanelIndex, int chromo) {
        validateChormoNumber(chromo);

        int i = chromo - startChr;

        return testTypeReducedOutDir.get(testTypeIndex).get(rPanelIndex).get(i);
    }

    /**
     * Method to access reducedFileName
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @param index
     * @return
     */
    public String getReducedFileName(int testTypeIndex, int rPanelIndex, int chromo, int index) {
        validateChormoNumber(chromo);

        int i = chromo - startChr;

        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // testTypeReducedFileName.get(rPanelIndex).get(i);
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFileName is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeReducedFile.get(testTypeIndex).get(rPanelIndex).get(i).get(index).getFullName();
    }

    /**
     * Method to access reducedFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @param index
     * @return
     */
    public String getReducedFile(int testTypeIndex, int rPanelIndex, int chromo, int index) {
        validateChormoNumber(chromo);

        int i = chromo - startChr;

        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeReducedFile.get(testTypeIndex).get(rPanelIndex).get(i).get(index).getFullName();
    }

    /**
     * Method to set the finalStatus of the reducedFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @param index
     * @param finalStatus
     */
    public void setReducedFileFinalStatus(int testTypeIndex, int rPanelIndex, int chromo, int index, String finalStatus) {
        validateChormoNumber(chromo);

        int i = chromo - startChr;

        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        testTypeReducedFile.get(testTypeIndex).get(rPanelIndex).get(i).get(index).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the reducedFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @param index
     * @return
     */
    public String getReducedFileFinalStatus(int testTypeIndex, int rPanelIndex, int chromo, int index) {
        validateChormoNumber(chromo);

        int i = chromo - startChr;

        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeReducedFile.get(testTypeIndex).get(rPanelIndex).get(i).get(index).getFinalStatus();
    }

    /**
     * Method to access reducedFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @return
     */
    public String getTheLastReducedFile(int testTypeIndex, int rPanelIndex, int chromo) {
        validateChormoNumber(chromo);

        int i = chromo - startChr;

        // ArrayList<String> tmpList = new ArrayList<String>();
        int lastIndex = testTypeReducedFile.get(testTypeIndex).get(rPanelIndex).get(i).size() - 1;
        // System.out.println("[MergeFiles] lastIndex size = " + lastIndex);

        return testTypeReducedFile.get(testTypeIndex).get(rPanelIndex).get(i).get(lastIndex).getFullName();
    }

    /**
     * Method to access the index of the reducedFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @return
     */
    public int getTheLastReducedFileIndex(int testTypeIndex, int rPanelIndex, int chromo) {
        validateChormoNumber(chromo);

        int i = chromo - startChr;

        // ArrayList<String> tmpList = new ArrayList<String>();
        int lastIndex = testTypeReducedFile.get(testTypeIndex).get(rPanelIndex).get(i).size() - 1;
        // System.out.println("[MergeFiles] lastIndex size = " + lastIndex);

        return lastIndex;
    }

    /**
     * Method to access print theLastReducedFiles
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     */
    public void printTheLastReducedFiles(int testTypeIndex, int rPanelIndex, int chromo) {
        // System.out.println("\t[MergeFiles] theLastReducedFiles ARE:");
        for (int hh = 0; hh <= testTypeIndex; hh++) {
            for (int kk = 0; kk <= rPanelIndex; kk++) {
                for (int j = 0; j <= chromo - startChr; j++) {
                    // System.out.println("\t[MergeFiles] " + testTypeReducedFile.get(rPanelIndex).get(kk).get(j));
                }
            }
        }
    }

    /**
     * Method to access filterByAllFileName
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @return
     */
    public String getFilterByAllFileName(int testTypeIndex, int rPanelIndex, int chromo) {
        validateChormoNumber(chromo);

        int i = chromo - startChr;

        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // testTypeReducedFileName.get(rPanelIndex).get(i);
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFileName is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeFilteredByAllFile.get(testTypeIndex).get(rPanelIndex).get(i).getFullName();
    }

    /**
     * Method to access filteredByAllFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @return
     */
    public String getFilteredByAllFile(int testTypeIndex, int rPanelIndex, int chromo) {
        validateChormoNumber(chromo);

        int i = chromo - startChr;

        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeFilteredByAllFile.get(testTypeIndex).get(rPanelIndex).get(i).getFullName();
    }

    /**
     * Method to set the finalStatus of the filteredByAllFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @param finalStatus
     */
    public void setFilteredByAllFileFinalStatus(int testTypeIndex, int rPanelIndex, int chromo, String finalStatus) {
        validateChormoNumber(chromo);

        int i = chromo - startChr;

        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        testTypeFilteredByAllFile.get(testTypeIndex).get(rPanelIndex).get(i).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the filteredByAllFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @return
     */
    public String getFilteredByAllFileFinalStatus(int testTypeIndex, int rPanelIndex, int chromo) {
        validateChormoNumber(chromo);

        int i = chromo - startChr;

        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeFilteredByAllFile.get(testTypeIndex).get(rPanelIndex).get(i).getFinalStatus();
    }

    /**
     * Method to access condensedFileName
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @return
     */
    public String getCondensedFileName(int testTypeIndex, int rPanelIndex, int chromo) {
        validateChormoNumber(chromo);

        int i = chromo - startChr;

        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // testTypeReducedFileName.get(rPanelIndex).get(i);
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFileName is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeCondensedFile.get(testTypeIndex).get(rPanelIndex).get(i).getFullName();
    }

    /**
     * Method to access condensedFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @return
     */
    public String getCondensedFile(int testTypeIndex, int rPanelIndex, int chromo) {
        validateChormoNumber(chromo);

        int i = chromo - startChr;

        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeCondensedFile.get(testTypeIndex).get(rPanelIndex).get(i).getFullName();
    }

    /**
     * Method to set the finalStatus of the condensedFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @param finalStatus
     */
    public void setCondensedFileFinalStatus(int testTypeIndex, int rPanelIndex, int chromo, String finalStatus) {
        validateChormoNumber(chromo);

        int i = chromo - startChr;

        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        testTypeCondensedFile.get(testTypeIndex).get(rPanelIndex).get(i).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the condensedFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @return
     */
    public String getCondensedFileFinalStatus(int testTypeIndex, int rPanelIndex, int chromo) {
        validateChormoNumber(chromo);

        int i = chromo - startChr;

        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeCondensedFile.get(testTypeIndex).get(rPanelIndex).get(i).getFinalStatus();
    }

    /**
     * Method to access additionalCondensedFileName
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param index
     * @return
     */
    public String getAdditionalCondensedFileName(int testTypeIndex, int rPanelIndex, int index) {
        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // testTypeReducedFileName.get(rPanelIndex).get(i);
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFileName is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeAdditionalCondensedFile.get(testTypeIndex).get(rPanelIndex).get(index).getFullName();
    }

    /**
     * Method to access additionalCondensedFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param index
     * @return
     */
    public String getAdditionalCondensedFile(int testTypeIndex, int rPanelIndex, int index) {
        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeAdditionalCondensedFile.get(testTypeIndex).get(rPanelIndex).get(index).getFullName();
    }

    /**
     * Method to set the finalStatus of the additionalCondensedFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param index
     * @param finalStatus
     */
    public void setAdditionalCondensedFileFinalStatus(int testTypeIndex, int rPanelIndex, int index, String finalStatus) {
        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        testTypeAdditionalCondensedFile.get(testTypeIndex).get(rPanelIndex).get(index).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the additionalCondensedFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param index
     * @return
     */
    public String getAdditionalCondensedFileFinalStatus(int testTypeIndex, int rPanelIndex, int index) {
        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeAdditionalCondensedFile.get(testTypeIndex).get(rPanelIndex).get(index).getFinalStatus();
    }

    /**
     * Method to access additionalFilteredByAllFileName
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param index
     * @return
     */
    public String getAdditionalFilteredByAllFileName(int testTypeIndex, int rPanelIndex, int index) {
        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // testTypeReducedFileName.get(rPanelIndex).get(i);
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFileName is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeAdditionalFilteredByAllFile.get(testTypeIndex).get(rPanelIndex).get(index).getFullName();
    }

    /**
     * Method to access additionalFilteredByAllFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param index
     * @return
     */
    public String getAdditionalFilteredByAllFile(int testTypeIndex, int rPanelIndex, int index) {
        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeAdditionalFilteredByAllFile.get(testTypeIndex).get(rPanelIndex).get(index).getFullName();
    }

    /**
     * Method to set the finalStatus of the additionalCondensedFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param index
     * @param finalStatus
     */
    public void setAdditionalFilteredByAllFileFinalStatus(int testTypeIndex, int rPanelIndex, int index, String finalStatus) {
        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        testTypeAdditionalFilteredByAllFile.get(testTypeIndex).get(rPanelIndex).get(index).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the additionalFilteredByAllFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param index
     * @return
     */
    public String getAdditionalFilteredByAllFileFinalStatus(int testTypeIndex, int rPanelIndex, int index) {
        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeAdditionalFilteredByAllFile.get(testTypeIndex).get(rPanelIndex).get(index).getFinalStatus();
    }

    /**
     * Method to access additionalFilteredByAllXFileName
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param index
     * @return
     */
    public String getAdditionalFilteredByAllXFileName(int testTypeIndex, int rPanelIndex, int index) {
        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // testTypeReducedFileName.get(rPanelIndex).get(i);
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFileName is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeAdditionalFilteredByAllXFile.get(testTypeIndex).get(rPanelIndex).get(index).getFullName();
    }

    /**
     * Method to access additionalFilteredByAllXFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param index
     * @return
     */
    public String getAdditionalFilteredByAllXFile(int testTypeIndex, int rPanelIndex, int index) {
        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeAdditionalFilteredByAllXFile.get(testTypeIndex).get(rPanelIndex).get(index).getFullName();
    }

    /**
     * Method to set the finalStatus of the additionalCondensedFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param index
     * @param finalStatus
     */
    public void setAdditionalFilteredByAllXFileFinalStatus(int testTypeIndex, int rPanelIndex, int index, String finalStatus) {
        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        testTypeAdditionalFilteredByAllXFile.get(testTypeIndex).get(rPanelIndex).get(index).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the additionalFilteredByAllXFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param index
     * @return
     */
    public String getAdditionalFilteredByAllXFileFinalStatus(int testTypeIndex, int rPanelIndex, int index) {
        // TODO: IMPORTANT: Verify the index range!!!!
        // ArrayList<String> tmpList = new ArrayList<String>();
        // tmpList = (testTypeReducedFileName.get(rPanelIndex, i));
        // int lastIndex = tmpList.size() - 1;

        // if(index > lastIndex) {
        // System.err.println("[MergeFiles] Error, the number of testTypeReducedFile is greater than the existing in
        // chromosome " + chromo);
        // System.err.println(" index " + index + " > lastIndex = " + lastIndex);
        // System.exit(1);
        // }

        return testTypeAdditionalFilteredByAllXFile.get(testTypeIndex).get(rPanelIndex).get(index).getFinalStatus();
    }

    /**
     * Method to access the last CondensedFile of each testType and rPanel
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getFinalCondensedFile(int testTypeIndex, int rPanelIndex) {
        int lastIndex = testTypeAdditionalCondensedIndex.get(testTypeIndex).get(rPanelIndex);
        return testTypeAdditionalCondensedFile.get(testTypeIndex).get(rPanelIndex).get(lastIndex - 1).getFullName();
    }

    /**
     * Method to access the last FilteredByAllFile of each testType and rPanel
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getFinalFilteredByAllFile(int testTypeIndex, int rPanelIndex) {
        int lastIndex = testTypeAdditionalFilteredByAllIndex.get(testTypeIndex).get(rPanelIndex);
        return testTypeAdditionalFilteredByAllFile.get(testTypeIndex).get(rPanelIndex).get(lastIndex - 1).getFullName();
    }

    /*
     * // Method to access the last FilteredByAllXFile of each testType and rPanel public String
     * getFinalFilteredByAllXFile(int testTypeIndex, int rPanelIndex) {
     * 
     * int lastIndex = testTypeAdditionalFilteredByAllXIndex.get(testTypeIndex).get(rPanelIndex); return
     * testTypeAdditionalFilteredByAllXFile.get(testTypeIndex).get(rPanelIndex).get(lastIndex-1).getFullName(); }
     */

    /**
     * Private method to validate the chromosome access to all public methods
     * 
     * @param chromoNumber
     */
    private void validateChormoNumber(int chromoNumber) {
        if (chromoNumber < 1 || chromoNumber > ChromoInfo.MAX_NUMBER_OF_CHROMOSOMES) {
            System.err.println("[MergeFiles] Error, chromosome " + chromoNumber + " does not exist");
            System.exit(1);
        }
    }
}
