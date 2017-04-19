package guidance.files;

import java.util.List;

import guidance.utils.GenericFile;
import guidance.utils.ParseCmdLine;

import java.io.File;
import java.util.ArrayList;


public class ResultsFiles {

    private static final String EXTENSION_TXT = ".txt";
    private static final String EXTENSION_PDF = ".pdf";
    private static final String EXTENSION_TXT_GZ = ".txt.gz";
    private static final String EXTENSION_TIFF = ".tiff";

    private ArrayList<ArrayList<String>> testTypeListOutDir = new ArrayList<>();

    private ArrayList<ArrayList<GenericFile>> testTypeTopHitsFile = new ArrayList<>();
    private ArrayList<ArrayList<GenericFile>> testTypeCorrectedPvaluesFile = new ArrayList<>();
    private ArrayList<ArrayList<GenericFile>> testTypeQqPlotPdfFile = new ArrayList<>();
    private ArrayList<ArrayList<GenericFile>> testTypeManhattanPdfFile = new ArrayList<>();
    private ArrayList<ArrayList<GenericFile>> testTypeQqPlotTiffFile = new ArrayList<>();
    private ArrayList<ArrayList<GenericFile>> testTypeManhattanTiffFile = new ArrayList<>();


    /**
     * Constructor for result files
     * 
     * @param parsingArgs
     * @param baseOutDir
     * @param refPanels
     */
    public ResultsFiles(ParseCmdLine parsingArgs, String baseOutDir, List<String> refPanels) {
        int numberOfTestTypesNames = parsingArgs.getNumberOfTestTypeName();

        // We create the first directory name: the cohort directory
        String mixedCohort = parsingArgs.getCohort();

        for (int tt = 0; tt < numberOfTestTypesNames; tt++) {
            String testTypeName = parsingArgs.getTestTypeName(tt);
            String testTypeOutDir = baseOutDir + File.separator + "associations" + File.separator + testTypeName;

            ArrayList<String> rpanelListOutDir = new ArrayList<>();

            ArrayList<GenericFile> rpanelTopHitsFile = new ArrayList<>();
            ArrayList<GenericFile> rpanelCorrectedPvaluesFile = new ArrayList<>();

            ArrayList<GenericFile> rpanelQqPlotPdfFile = new ArrayList<>();
            ArrayList<GenericFile> rpanelManhattanPdfFile = new ArrayList<>();

            ArrayList<GenericFile> rpanelQqPlotTiffFile = new ArrayList<>();
            ArrayList<GenericFile> rpanelManhattanTiffFile = new ArrayList<>();

            for (int j = 0; j < refPanels.size(); j++) {
                String rPanel = refPanels.get(j);
                // String rpanelOutDir = testTypeOutDir + File.separator + mixedCohort + "_for_" + rPanel;
                String rpanelOutDirSummary = testTypeOutDir + File.separator + mixedCohort + "_for_" + rPanel + File.separator + "summary";

                String tmpTopHitsFileName = "tophits_" + testTypeName + "_" + mixedCohort + "_" + rPanel + EXTENSION_TXT_GZ;
                // String tmpTopHitsFile = rpanelOutDirSummary + File.separator + tmpTopHitsFileName;
                GenericFile myTopHitsFile = new GenericFile(rpanelOutDirSummary, tmpTopHitsFileName, "compressed", "none");
                rpanelTopHitsFile.add(myTopHitsFile);

                String tmpCorrectedPvaluesFileName = "corrected_pvalues_" + testTypeName + "_" + mixedCohort + "_" + rPanel + EXTENSION_TXT;
                // String tmpCorrectedPvaluesFile = rpanelOutDirSummary + File.separator + tmpCorrectedPvaluesFileName;
                GenericFile myCorrectedPvaluesFile = new GenericFile(rpanelOutDirSummary, tmpCorrectedPvaluesFileName, "uncompressed",
                        "none");
                rpanelCorrectedPvaluesFile.add(myCorrectedPvaluesFile);

                String tmpQqPlotPdfFileName = "QQplot_" + testTypeName + "_" + mixedCohort + "_" + rPanel + EXTENSION_PDF;
                // String tmpQqPlotPdfFile = rpanelOutDirSummary + File.separator + tmpQqPlotPdfFileName;
                GenericFile myQqPlotPdfFile = new GenericFile(rpanelOutDirSummary, tmpQqPlotPdfFileName, "uncompressed", "none");
                rpanelQqPlotPdfFile.add(myQqPlotPdfFile);

                String tmpManhattanPdfFileName = "manhattan_" + testTypeName + "_" + mixedCohort + "_" + rPanel + EXTENSION_PDF;
                // String tmpManhattanPdfFile = rpanelOutDirSummary + File.separator + tmpManhattanPdfFileName;
                GenericFile myManhattanPdfFile = new GenericFile(rpanelOutDirSummary, tmpManhattanPdfFileName, "uncompressed", "none");
                rpanelManhattanPdfFile.add(myManhattanPdfFile);

                String tmpQqPlotTiffFileName = "QQplot_" + testTypeName + "_" + mixedCohort + "_" + rPanel + EXTENSION_TIFF;
                // String tmpQqPlotTiffFile = rpanelOutDirSummary + File.separator + tmpQqPlotTiffFileName;
                GenericFile myQqPlotTiffFile = new GenericFile(rpanelOutDirSummary, tmpQqPlotTiffFileName, "uncompressed", "none");
                rpanelQqPlotTiffFile.add(myQqPlotTiffFile);

                String tmpManhattanTiffFileName = "manhattan_" + testTypeName + "_" + mixedCohort + "_" + rPanel + EXTENSION_TIFF;
                // String tmpManhattanTiffFile = rpanelOutDirSummary + File.separator + tmpManhattanTiffFileName;
                GenericFile myManhattanTiffFile = new GenericFile(rpanelOutDirSummary, tmpManhattanTiffFileName, "uncompressed", "none");
                rpanelManhattanTiffFile.add(myManhattanTiffFile);

            } // End of for panels
              
            // Now we have to build the list of reduced files for the type of Test. We store this list
            testTypeListOutDir.add(rpanelListOutDir);
            testTypeTopHitsFile.add(rpanelTopHitsFile);
            testTypeCorrectedPvaluesFile.add(rpanelCorrectedPvaluesFile);

            testTypeQqPlotPdfFile.add(rpanelQqPlotPdfFile);
            testTypeManhattanPdfFile.add(rpanelManhattanPdfFile);

            testTypeQqPlotTiffFile.add(rpanelQqPlotTiffFile);
            testTypeManhattanTiffFile.add(rpanelManhattanTiffFile);

        } // End of for test types
    }

    /**
     * Method to access correctedPvaluesFile information
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getListOutDir(int testTypeIndex, int rPanelIndex) {
        return testTypeListOutDir.get(testTypeIndex).get(rPanelIndex);
    }

    /**
     * Method to access topHitsFileName
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getTopHitsFileName(int testTypeIndex, int rPanelIndex) {
        return testTypeTopHitsFile.get(testTypeIndex).get(rPanelIndex).getName();
    }

    /**
     * Method to access topHitsFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getTopHitsFile(int testTypeIndex, int rPanelIndex) {
        return testTypeTopHitsFile.get(testTypeIndex).get(rPanelIndex).getFullName();
    }

    /**
     * Method to set the finalStatus of the topHitsFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param finalStatus
     */
    public void setTopHitsFileFinalStatus(int testTypeIndex, int rPanelIndex, String finalStatus) {
        testTypeTopHitsFile.get(testTypeIndex).get(rPanelIndex).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the topHitsFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getTopHitsFileFinalStatus(int testTypeIndex, int rPanelIndex) {
        return testTypeTopHitsFile.get(testTypeIndex).get(rPanelIndex).getFinalStatus();
    }

    /**
     * Method to access CorrectedPvaluesFileName
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getCorrectedPvaluesFileName(int testTypeIndex, int rPanelIndex) {
        return testTypeCorrectedPvaluesFile.get(testTypeIndex).get(rPanelIndex).getName();
    }

    /**
     * Method to access CorrectedPvaluesFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getCorrectedPvaluesFile(int testTypeIndex, int rPanelIndex) {
        return testTypeCorrectedPvaluesFile.get(testTypeIndex).get(rPanelIndex).getFullName();
    }

    /**
     * Method to set the finalStatus of the CorrectedPvaluesFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param finalStatus
     */
    public void setCorrectedPvaluesFinalStatus(int testTypeIndex, int rPanelIndex, String finalStatus) {
        testTypeCorrectedPvaluesFile.get(testTypeIndex).get(rPanelIndex).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the CorrectedPvaluesFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getCorrectedPvaluesFileFinalStatus(int testTypeIndex, int rPanelIndex) {
        return testTypeCorrectedPvaluesFile.get(testTypeIndex).get(rPanelIndex).getFinalStatus();
    }

    /**
     * Method to access qqPlotPdfFileName
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getQqPlotPdfFileName(int testTypeIndex, int rPanelIndex) {
        return testTypeQqPlotPdfFile.get(testTypeIndex).get(rPanelIndex).getName();
    }

    /**
     * Method to access qqPlotPdfFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getQqPlotPdfFile(int testTypeIndex, int rPanelIndex) {
        return testTypeQqPlotPdfFile.get(testTypeIndex).get(rPanelIndex).getFullName();
    }

    /**
     * Method to set the finalStatus of the qqPlotPdfFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param finalStatus
     */
    public void setQqPlotPdfFileFinalStatus(int testTypeIndex, int rPanelIndex, String finalStatus) {
        testTypeQqPlotPdfFile.get(testTypeIndex).get(rPanelIndex).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the qqPlotPdfFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getQqPlotPdfFileFinalStatus(int testTypeIndex, int rPanelIndex) {
        return testTypeQqPlotPdfFile.get(testTypeIndex).get(rPanelIndex).getFinalStatus();
    }

    /**
     * Method to access qqPlotTiffFileName
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getQqPlotTiffFileName(int testTypeIndex, int rPanelIndex) {
        return testTypeQqPlotTiffFile.get(testTypeIndex).get(rPanelIndex).getName();
    }

    /**
     * Method to access qqPlotTiffFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getQqPlotTiffFile(int testTypeIndex, int rPanelIndex) {
        return testTypeQqPlotTiffFile.get(testTypeIndex).get(rPanelIndex).getFullName();
    }

    /**
     * Method to set the finalStatus of the qqPlotPdfFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param finalStatus
     */
    public void setQqPlotTiffFileFinalStatus(int testTypeIndex, int rPanelIndex, String finalStatus) {
        testTypeQqPlotTiffFile.get(testTypeIndex).get(rPanelIndex).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the qqPlotTiffFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getQqPlotTiffFileFinalStatus(int testTypeIndex, int rPanelIndex) {
        return testTypeQqPlotTiffFile.get(testTypeIndex).get(rPanelIndex).getFinalStatus();
    }

    /**
     * Method to access mahattanPdfFileName
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getManhattanPdfFileName(int testTypeIndex, int rPanelIndex) {
        return testTypeManhattanPdfFile.get(testTypeIndex).get(rPanelIndex).getName();
    }

    /**
     * Method to access manhattanPdfFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getManhattanPdfFile(int testTypeIndex, int rPanelIndex) {
        return testTypeManhattanPdfFile.get(testTypeIndex).get(rPanelIndex).getFullName();
    }

    /**
     * Method to set the finalStatus of the manhattanPdfFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param finalStatus
     */
    public void setManhattanPdfFileFinalStatus(int testTypeIndex, int rPanelIndex, String finalStatus) {
        testTypeManhattanPdfFile.get(testTypeIndex).get(rPanelIndex).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the manhattanPdfFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getManhattanPdfFileFinalStatus(int testTypeIndex, int rPanelIndex) {
        return testTypeManhattanPdfFile.get(testTypeIndex).get(rPanelIndex).getFinalStatus();
    }

    /**
     * Method to access manhattanTiffFileName
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getManhattanTiffFileName(int testTypeIndex, int rPanelIndex) {
        return testTypeManhattanTiffFile.get(testTypeIndex).get(rPanelIndex).getName();
    }

    /**
     * Method to access manhattanTiffFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getManhattanTiffFile(int testTypeIndex, int rPanelIndex) {
        return testTypeManhattanTiffFile.get(testTypeIndex).get(rPanelIndex).getFullName();
    }

    /**
     * Method to set the finalStatus of the manhattanPdfFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param finalStatus
     */
    public void setManhattanTiffFileFinalStatus(int testTypeIndex, int rPanelIndex, String finalStatus) {
        testTypeManhattanTiffFile.get(testTypeIndex).get(rPanelIndex).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of the manhattanTiffFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @return
     */
    public String getManhattanTiffFileFinalStatus(int testTypeIndex, int rPanelIndex) {
        return testTypeManhattanTiffFile.get(testTypeIndex).get(rPanelIndex).getFinalStatus();
    }

}
