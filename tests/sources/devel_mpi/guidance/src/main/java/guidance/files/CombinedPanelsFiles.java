package guidance.files;

import java.util.List;

import guidance.utils.ChromoInfo;
import guidance.utils.GenericFile;
import guidance.utils.ParseCmdLine;

import java.io.File;
import java.util.ArrayList;


public class CombinedPanelsFiles {

    private static final String EXTENSION_TXT = ".txt";
    private static final String EXTENSION_PDF = ".pdf";
    private static final String EXTENSION_TXT_GZ = ".txt.gz";
    private static final String EXTENSION_TIFF = ".tiff";

    private ArrayList<String> testTypeListOutDir = new ArrayList<>();

    private ArrayList<ArrayList<GenericFile>> testTypeCombinedFilteredByAllFile = new ArrayList<>();
    private ArrayList<ArrayList<GenericFile>> testTypeCombinedFilteredByAllXFile = new ArrayList<>();

    private ArrayList<GenericFile> testTypeCombinedCondensedFile = new ArrayList<>();

    private ArrayList<GenericFile> testTypeTopHitsFile = new ArrayList<>();
    private ArrayList<GenericFile> testTypeCorrectedPvaluesFile = new ArrayList<>();
    private ArrayList<GenericFile> testTypeQqPlotPdfFile = new ArrayList<>();
    private ArrayList<GenericFile> testTypeManhattanPdfFile = new ArrayList<>();
    private ArrayList<GenericFile> testTypeQqPlotTiffFile = new ArrayList<>();
    private ArrayList<GenericFile> testTypeManhattanTiffFile = new ArrayList<>();


    /**
     * Constructor for combined panels files
     * 
     * @param parsingArgs
     * @param baseOutDir
     * @param refPanels
     */
    public CombinedPanelsFiles(ParseCmdLine parsingArgs, String baseOutDir, List<String> refPanels) {
        int startChr = parsingArgs.getStart();
        int endChr = parsingArgs.getEnd();
        int endChrNormal = endChr;
        if (endChr == 23) {
            endChrNormal = 22;
        }

        String startChrS = Integer.toString(startChr);
        String endChrS = Integer.toString(endChr);
        String endChrNormalS = Integer.toString(endChrNormal);

        int numberOfTestTypesNames = parsingArgs.getNumberOfTestTypeName();

        // We create the first directory name: the cohort directory
        String mixedCohort = parsingArgs.getCohort();

        for (int tt = 0; tt < numberOfTestTypesNames; tt++) {
            String testTypeName = parsingArgs.getTestTypeName(tt);
            String testTypeOutDir = baseOutDir + File.separator + "associations" + File.separator + testTypeName + File.separator
                    + mixedCohort + "_combined_panels";

            // ArrayList<String> rpanelListOutDir = new ArrayList<>();

            // ArrayList<GenericFile> rpanelTopHitsFile = new ArrayList<>();
            ArrayList<GenericFile> combinedFilteredByAllFile = new ArrayList<>();

            ArrayList<GenericFile> combinedFilteredByAllXFile = new ArrayList<>();

            // ArrayList<GenericFile> rpanelQqPlotPdfFile = new ArrayList<>();
            // ArrayList<GenericFile> rpanelManhattanPdfFile = new ArrayList<>();

            // ArrayList<GenericFile> rpanelQqPlotTiffFile = new ArrayList<>();
            // ArrayList<GenericFile> rpanelManhattanTiffFile = new ArrayList<>();
            String rPanel = null;
            for (int j = 0; j < refPanels.size(); j++) {
                rPanel = refPanels.get(j);
                testTypeOutDir = testTypeOutDir + "_" + rPanel;
            }

            rPanel = refPanels.get(0);
            String prefixFilteredName = "filteredByAll_results_" + testTypeName + "_" + mixedCohort + "_" + rPanel;
            String prefixCondensedName = "condensed_results_" + testTypeName + "_" + mixedCohort + "_" + rPanel;
            String prefixTopHitsName = "tophits_" + testTypeName + "_" + mixedCohort + "_" + rPanel;
            String prefixQqPlotName = "QQplot_" + testTypeName + "_" + mixedCohort + "_" + rPanel;
            String prefixManhattanName = "manhattan_" + testTypeName + "_" + mixedCohort + "_" + rPanel;
            String prefixCorrectedPvaluesName = "corrected_pvalues_" + testTypeName + "_" + mixedCohort + "_" + rPanel;

            for (int j = 1; j < refPanels.size(); j++) {
                rPanel = refPanels.get(j);
                prefixFilteredName = prefixFilteredName + "_" + rPanel;
                prefixCondensedName = prefixCondensedName + "_" + rPanel;
                prefixTopHitsName = prefixTopHitsName + "_" + rPanel;
                prefixQqPlotName = prefixQqPlotName + "_" + rPanel;
                prefixManhattanName = prefixManhattanName + "_" + rPanel;
                prefixCorrectedPvaluesName = prefixCorrectedPvaluesName + "_" + rPanel;

                String tmpCombinedFilteredByAllFileName = null;
                // String tmpCombinedFilteredByAllFile = null;
                if (startChr == endChrNormal) {
                    tmpCombinedFilteredByAllFileName = prefixFilteredName + "_chr_" + startChrS + EXTENSION_TXT_GZ;
                    // tmpCombinedFilteredByAllFile = testTypeOutDir + File.separator +
                    // tmpCombinedFilteredByAllFileName;
                } else {
                    tmpCombinedFilteredByAllFileName = prefixFilteredName + "_chr_" + startChrS + "_to_" + endChrNormalS + EXTENSION_TXT_GZ;
                    // tmpCombinedFilteredByAllFile = testTypeOutDir + File.separator +
                    // tmpCombinedFilteredByAllFileName;
                }
                GenericFile myCombinedFilteredByAllFile = new GenericFile(testTypeOutDir, tmpCombinedFilteredByAllFileName, "compressed",
                        "none");
                combinedFilteredByAllFile.add(myCombinedFilteredByAllFile);

                // System.out.println("[CombinedPanelsFiles] " + testTypeOutDir + "/" +
                // tmpCombinedFilteredByAllFileName);

                // If we are going to process chr 23, then prepare file names for it.
                if (endChr == ChromoInfo.MAX_NUMBER_OF_CHROMOSOMES) {
                    String tmpCombinedFilteredByAllXFileName = prefixFilteredName + "_chr_" + endChr + EXTENSION_TXT_GZ;
                    // String tmpCombinedFilteredByAllXFile = testTypeOutDir + File.separator +
                    // tmpCombinedFilteredByAllXFileName;
                    GenericFile myCombinedFilteredByAllXFile = new GenericFile(testTypeOutDir, tmpCombinedFilteredByAllXFileName,
                            "compressed", "none");
                    combinedFilteredByAllXFile.add(myCombinedFilteredByAllXFile);
                }
            } // End of for(j=0; j<refPanels.size();j++)
              // Now we have to build the list of reduced files for the type of Test. We store this list

            testTypeListOutDir.add(testTypeOutDir);
            testTypeCombinedFilteredByAllFile.add(combinedFilteredByAllFile);

            if (endChr == ChromoInfo.MAX_NUMBER_OF_CHROMOSOMES) {
                testTypeCombinedFilteredByAllXFile.add(combinedFilteredByAllXFile);
            }

            String tmpCombinedCondensedFileName = null;
            // String tmpCombinedCondensedFile = null;
            if (startChr == endChr) {
                tmpCombinedCondensedFileName = prefixCondensedName + "_chr_" + startChrS + EXTENSION_TXT_GZ;
                // tmpCombinedCondensedFile = testTypeOutDir + File.separator + tmpCombinedCondensedFileName;
            } else {
                tmpCombinedCondensedFileName = prefixCondensedName + "_chr_" + startChrS + "_to_" + endChrS + EXTENSION_TXT_GZ;
                // tmpCombinedCondensedFile = testTypeOutDir + File.separator + tmpCombinedCondensedFileName;
            }
            GenericFile myCombinedCondensedFile = new GenericFile(testTypeOutDir, tmpCombinedCondensedFileName, "compressed", "none");
            testTypeCombinedCondensedFile.add(myCombinedCondensedFile);

            String tmpTopHitsFileName = null;
            // String tmpTopHitsFile = null;
            if (startChr == endChr) {
                tmpTopHitsFileName = prefixTopHitsName + "_chr_" + startChrS + EXTENSION_TXT_GZ;
                // tmpTopHitsFile = testTypeOutDir + File.separator + tmpTopHitsFileName;
            } else {
                tmpTopHitsFileName = prefixTopHitsName + "_chr_" + startChrS + "_to_" + endChrS + EXTENSION_TXT_GZ;
                // tmpTopHitsFile = testTypeOutDir + File.separator + tmpTopHitsFileName;
            }
            GenericFile myTopHitsFile = new GenericFile(testTypeOutDir, tmpTopHitsFileName, "compressed", "none");
            testTypeTopHitsFile.add(myTopHitsFile);

            String tmpQqPlotPdfFileName = null;
            // String tmpQqPlotPdfFile = null;
            if (startChr == endChr) {
                tmpQqPlotPdfFileName = prefixQqPlotName + "_chr_" + startChrS + EXTENSION_PDF;
                // tmpQqPlotPdfFile = testTypeOutDir + File.separator + tmpQqPlotPdfFileName;
            } else {
                tmpQqPlotPdfFileName = prefixQqPlotName + "_chr_" + startChrS + "_to_" + endChrS + EXTENSION_PDF;
                // tmpQqPlotPdfFile = testTypeOutDir + File.separator + tmpQqPlotPdfFileName;
            }
            GenericFile myQqPlotPdfFile = new GenericFile(testTypeOutDir, tmpQqPlotPdfFileName, "compressed", "none");
            testTypeQqPlotPdfFile.add(myQqPlotPdfFile);

            String tmpQqPlotTiffFileName = null;
            // String tmpQqPlotTiffFile = null;
            if (startChr == endChr) {
                tmpQqPlotTiffFileName = prefixQqPlotName + "_chr_" + startChrS + EXTENSION_TIFF;
                // tmpQqPlotTiffFile = testTypeOutDir + File.separator + tmpQqPlotTiffFileName;
            } else {
                tmpQqPlotTiffFileName = prefixQqPlotName + "_chr_" + startChrS + "_to_" + endChrS + EXTENSION_TIFF;
                // tmpQqPlotTiffFile = testTypeOutDir + File.separator + tmpQqPlotTiffFileName;
            }
            GenericFile myQqPlotTiffFile = new GenericFile(testTypeOutDir, tmpQqPlotTiffFileName, "compressed", "none");
            testTypeQqPlotTiffFile.add(myQqPlotTiffFile);

            String tmpManhattanPdfFileName = null;
            // String tmpManhattanPdfFile = null;
            if (startChr == endChr) {
                tmpManhattanPdfFileName = prefixManhattanName + "_chr_" + startChrS + EXTENSION_PDF;
                // tmpManhattanPdfFile = testTypeOutDir + File.separator + tmpManhattanPdfFileName;
            } else {
                tmpManhattanPdfFileName = prefixManhattanName + "_chr_" + startChrS + "_to_" + endChrS + EXTENSION_PDF;
                // tmpManhattanPdfFile = testTypeOutDir + File.separator + tmpManhattanPdfFileName;
            }
            GenericFile myManhattanPdfFile = new GenericFile(testTypeOutDir, tmpManhattanPdfFileName, "compressed", "none");
            testTypeManhattanPdfFile.add(myManhattanPdfFile);

            String tmpManhattanTiffFileName = null;
            // String tmpManhattanTiffFile = null;
            if (startChr == endChr) {
                tmpManhattanTiffFileName = prefixManhattanName + "_chr_" + startChrS + EXTENSION_TIFF;
                // tmpManhattanTiffFile = testTypeOutDir + File.separator + tmpManhattanTiffFileName;
            } else {
                tmpManhattanTiffFileName = prefixManhattanName + "_chr_" + startChrS + "_to_" + endChrS + EXTENSION_TIFF;
                // tmpManhattanTiffFile = testTypeOutDir + File.separator + tmpManhattanTiffFileName;
            }
            GenericFile myManhattanTiffFile = new GenericFile(testTypeOutDir, tmpManhattanTiffFileName, "compressed", "none");
            testTypeManhattanTiffFile.add(myManhattanTiffFile);

            String tmpCorrectedPvaluesFileName = null;
            // String tmpCorrectedPvaluesFile = null;
            if (startChr == endChr) {
                tmpCorrectedPvaluesFileName = prefixCorrectedPvaluesName + "_chr_" + startChrS + EXTENSION_TXT;
                // tmpCorrectedPvaluesFile = testTypeOutDir + File.separator + tmpCorrectedPvaluesFileName;
            } else {
                tmpCorrectedPvaluesFileName = prefixCorrectedPvaluesName + "_chr_" + startChrS + "_to_" + endChrS + EXTENSION_TXT;
                // tmpCorrectedPvaluesFile = testTypeOutDir + File.separator + tmpCorrectedPvaluesFileName;
            }

            GenericFile myCorrectedPvaluesFile = new GenericFile(testTypeOutDir, tmpCorrectedPvaluesFileName, "compressed", "none");
            testTypeCorrectedPvaluesFile.add(myCorrectedPvaluesFile);

        } // End for of test types
    }

    /**
     * Method to access correctedPvaluesFile information
     * 
     * @param testTypeIndex
     * @return
     */
    public String getCombinedOutDir(int testTypeIndex) {
        return testTypeListOutDir.get(testTypeIndex);
    }

    /**
     * Method to access testTypeCombinedFilteredByAllFileName
     * 
     * @param testTypeIndex
     * @param index
     * @return
     */
    public String getCombinedFilteredByAllFileName(int testTypeIndex, int index) {
        return testTypeCombinedFilteredByAllFile.get(testTypeIndex).get(index).getName();
    }

    /**
     * Method to access testTypeCombinedFilteredByAllFile
     * 
     * @param testTypeIndex
     * @param index
     * @return
     */
    public String getCombinedFilteredByAllFile(int testTypeIndex, int index) {
        return testTypeCombinedFilteredByAllFile.get(testTypeIndex).get(index).getFullName();
    }

    /**
     * Method to set the finalStatus of the testTypeCorrectedPvaluesFile
     * 
     * @param testTypeIndex
     * @param index
     * @param finalStatus
     */
    public void setCombinedFilteredByAllFileFinalStatus(int testTypeIndex, int index, String finalStatus) {
        testTypeCombinedFilteredByAllFile.get(testTypeIndex).get(index).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the testTypeCombinedFilteredByAllFile
     * 
     * @param testTypeIndex
     * @param index
     * @return
     */
    public String getCombinedFilteredByAllFileFinalStatus(int testTypeIndex, int index) {
        return testTypeCombinedFilteredByAllFile.get(testTypeIndex).get(index).getFinalStatus();
    }

    /**
     * Method to access testTypeCombinedFilteredByAllFile
     * 
     * @param testTypeIndex
     * @param index
     * @return
     */
    public String getCombinedFilteredByAllXFile(int testTypeIndex, int index) {
        return testTypeCombinedFilteredByAllXFile.get(testTypeIndex).get(index).getFullName();
    }

    /**
     * Method to set the finalStatus of the testTypeCorrectedPvaluesFile
     * 
     * @param testTypeIndex
     * @param index
     * @param finalStatus
     */
    public void setCombinedFilteredByAllXFileFinalStatus(int testTypeIndex, int index, String finalStatus) {
        testTypeCombinedFilteredByAllXFile.get(testTypeIndex).get(index).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the testTypeCombinedFilteredByAllFile
     * 
     * @param testTypeIndex
     * @param index
     * @return
     */
    public String getCombinedFilteredByAllXFileFinalStatus(int testTypeIndex, int index) {
        return testTypeCombinedFilteredByAllXFile.get(testTypeIndex).get(index).getFinalStatus();
    }

    /**
     * Method to access testTypeCombinedCondensedFileName
     * 
     * @param testTypeIndex
     * @return
     */
    public String getCombinedCondensedFileName(int testTypeIndex) {
        return testTypeCombinedCondensedFile.get(testTypeIndex).getName();
    }

    /**
     * Method to access testTypeCombinedCondensedFile
     * 
     * @param testTypeIndex
     * @return
     */
    public String getCombinedCondensedFile(int testTypeIndex) {
        return testTypeCombinedCondensedFile.get(testTypeIndex).getFullName();
    }

    /**
     * Method to set the finalStatus of the testTypeCombinedCondensedFile
     * 
     * @param testTypeIndex
     * @param finalStatus
     */
    public void setCombinedCondensedFileFinalStatus(int testTypeIndex, String finalStatus) {
        testTypeCombinedCondensedFile.get(testTypeIndex).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the testTypeCombinedCondensedFile
     * 
     * @param testTypeIndex
     * @return
     */
    public String getCombinedCondensedFileFinalStatus(int testTypeIndex) {
        return testTypeCombinedCondensedFile.get(testTypeIndex).getFinalStatus();
    }

    /**
     * Method to access testTypeTopHitsFileName
     * 
     * @param testTypeIndex
     * @return
     */
    public String getCombinedTopHitsFileName(int testTypeIndex) {
        return testTypeTopHitsFile.get(testTypeIndex).getName();
    }

    /**
     * Method to access testTypeTopHitsFile
     * 
     * @param testTypeIndex
     * @return
     */
    public String getTopHitsFile(int testTypeIndex) {
        return testTypeTopHitsFile.get(testTypeIndex).getFullName();
    }

    /**
     * Method to set the finalStatus of the testTypeTopHitsFile
     * 
     * @param testTypeIndex
     * @param finalStatus
     */
    public void setTopHitsFileFinalStatus(int testTypeIndex, String finalStatus) {
        testTypeTopHitsFile.get(testTypeIndex).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the testTypeTopHitsFile
     * 
     * @param testTypeIndex
     * @return
     */
    public String getTopHitsFileFinalStatus(int testTypeIndex) {
        return testTypeTopHitsFile.get(testTypeIndex).getFinalStatus();
    }

    /**
     * Method to access qqPlotPdfFileName
     * 
     * @param testTypeIndex
     * @return
     */
    public String getQqPlotPdfFileName(int testTypeIndex) {
        return testTypeQqPlotPdfFile.get(testTypeIndex).getName();
    }

    /**
     * Method to access qqPlotPdfFile
     * 
     * @param testTypeIndex
     * @return
     */
    public String getQqPlotPdfFile(int testTypeIndex) {
        return testTypeQqPlotPdfFile.get(testTypeIndex).getFullName();
    }

    /**
     * Method to set the finalStatus of the qqPlotPdfFile
     * 
     * @param testTypeIndex
     * @param finalStatus
     */
    public void setQqPlotPdfFileFinalStatus(int testTypeIndex, String finalStatus) {
        testTypeQqPlotPdfFile.get(testTypeIndex).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the qqPlotPdfFile
     * 
     * @param testTypeIndex
     * @return
     */
    public String getQqPlotPdfFileFinalStatus(int testTypeIndex) {
        return testTypeQqPlotPdfFile.get(testTypeIndex).getFinalStatus();
    }

    /**
     * Method to access qqPlotTiffFileName
     * 
     * @param testTypeIndex
     * @return
     */
    public String getQqPlotTiffFileName(int testTypeIndex) {
        return testTypeQqPlotTiffFile.get(testTypeIndex).getName();
    }

    /**
     * Method to access qqPlotTiffFile
     * 
     * @param testTypeIndex
     * @return
     */
    public String getQqPlotTiffFile(int testTypeIndex) {
        return testTypeQqPlotTiffFile.get(testTypeIndex).getFullName();
    }

    /**
     * Method to set the finalStatus of the qqPlotPdfFile
     * 
     * @param testTypeIndex
     * @param finalStatus
     */
    public void setQqPlotTiffFileFinalStatus(int testTypeIndex, String finalStatus) {
        testTypeQqPlotTiffFile.get(testTypeIndex).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the qqPlotTiffFile
     * 
     * @param testTypeIndex
     * @return
     */
    public String getQqPlotTiffFileFinalStatus(int testTypeIndex) {
        return testTypeQqPlotTiffFile.get(testTypeIndex).getFinalStatus();
    }

    /**
     * Method to access mahattanPdfFileName
     * 
     * @param testTypeIndex
     * @return
     */
    public String getManhattanPdfFileName(int testTypeIndex) {
        return testTypeManhattanPdfFile.get(testTypeIndex).getName();
    }

    /**
     * Method to access manhattanPdfFile
     * 
     * @param testTypeIndex
     * @return
     */
    public String getManhattanPdfFile(int testTypeIndex) {
        return testTypeManhattanPdfFile.get(testTypeIndex).getFullName();
    }

    /**
     * Method to access mahattanTiffFileName
     * 
     * @param testTypeIndex
     * @return
     */
    public String getManhattanTiffFileName(int testTypeIndex) {
        return testTypeManhattanTiffFile.get(testTypeIndex).getName();
    }

    /**
     * Method to access manhattanTiffFile
     * 
     * @param testTypeIndex
     * @return
     */
    public String getManhattanTiffFile(int testTypeIndex) {
        return testTypeManhattanTiffFile.get(testTypeIndex).getFullName();
    }

    /**
     * Method to access CorrectedPvaluesFileName
     * 
     * @param testTypeIndex
     * @return
     */
    public String getCorrectedPvaluesFileName(int testTypeIndex) {
        return testTypeCorrectedPvaluesFile.get(testTypeIndex).getName();
    }

    /**
     * Method to access CorrectedPvaluesFile
     * 
     * @param testTypeIndex
     * @return
     */
    public String getCorrectedPvaluesFile(int testTypeIndex) {
        return testTypeCorrectedPvaluesFile.get(testTypeIndex).getFullName();
    }

    /**
     * Method to set the finalStatus of the CorrectedPvaluesFile
     * 
     * @param testTypeIndex
     * @param finalStatus
     */
    public void setCorrectedPvaluesFinalStatus(int testTypeIndex, String finalStatus) {
        testTypeCorrectedPvaluesFile.get(testTypeIndex).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the CorrectedPvaluesFile
     * 
     * @param testTypeIndex
     * @return
     */
    public String getCorrectedPvaluesFileFinalStatus(int testTypeIndex) {
        return testTypeCorrectedPvaluesFile.get(testTypeIndex).getFinalStatus();
    }

}
