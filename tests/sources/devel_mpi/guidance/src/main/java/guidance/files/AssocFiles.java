/** @file 
 *  Copyright 2002-2014 Barcelona Supercomputing Center (www.bsc.es)
 *  Life Science Department, 
 *  Computational Genomics Group (http://www.bsc.es/life-sciences/computational-genomics)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 *  Last update: $LastChangedDate: 2015-01-08 12:51:53 +0100 (Thu, 08 Jan 2015) $
 *  Revision Number: $Revision: 15 $
 *  Last revision  : $LastChangedRevision: 15 $
 *  Written by     : Friman Sanchez C.
 *                 : friman.sanchez@gmail.com
 *  Modified by    :
 *                
 *  Guidance web page: http://cg.bsc.es/guidance/
 */

package guidance.files;

import java.util.List;

import guidance.utils.ChromoInfo;
import guidance.utils.GenericFile;
import guidance.utils.ParseCmdLine;

import java.io.File;
import java.util.ArrayList;


public class AssocFiles {

    private ArrayList<ArrayList<ArrayList<String>>> outDir = new ArrayList<>();

    private ArrayList<ArrayList<ArrayList<ArrayList<GenericFile>>>> snptestOutFile = new ArrayList<>();
    private ArrayList<ArrayList<ArrayList<ArrayList<GenericFile>>>> snptestLogFileName = new ArrayList<>();
    private ArrayList<ArrayList<ArrayList<ArrayList<GenericFile>>>> snptestLogFile = new ArrayList<>();

    private ArrayList<ArrayList<ArrayList<ArrayList<GenericFile>>>> summaryFile = new ArrayList<>();

    private int startChr = 0;
    private int endChr = 0;


    /**
     * Creates a new AssocFiles instance
     * 
     * @param parsingArgs
     * @param generalChromoInfo
     * @param baseOutDir
     * @param refPanels
     */
    public AssocFiles(ParseCmdLine parsingArgs, ChromoInfo generalChromoInfo, String baseOutDir, List<String> refPanels) {
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
            ArrayList<ArrayList<ArrayList<GenericFile>>> rpanelListSnptestOutFile = new ArrayList<>();
            ArrayList<ArrayList<ArrayList<GenericFile>>> rpanelListSnptestLogFile = new ArrayList<>();
            ArrayList<ArrayList<ArrayList<GenericFile>>> rpanelListSummaryFile = new ArrayList<>();

            for (int j = 0; j < refPanels.size(); j++) {
                String rPanel = refPanels.get(j);
                String rpanelOutDir = testTypeOutDir + File.separator + mixedCohort + "_for_" + rPanel;

                ArrayList<String> chromoListOutDir = new ArrayList<String>();
                ArrayList<ArrayList<GenericFile>> chromoListSnptestOutFile = new ArrayList<>();
                ArrayList<ArrayList<GenericFile>> chromoListSnptestLogFile = new ArrayList<>();
                ArrayList<ArrayList<GenericFile>> chromoListSummaryFile = new ArrayList<>();

                for (int chromo = startChr; chromo <= endChr; chromo++) {
                    String tmpChrDir = rpanelOutDir + File.separator + "Chr_" + chromo;
                    chromoListOutDir.add(tmpChrDir);

                    int maxSize = generalChromoInfo.getMaxSize(chromo);
                    int total_chunks = maxSize / chunkSize;
                    int module = maxSize % chunkSize;
                    if (module != 0) {
                        total_chunks++;
                    }
                    int lim1 = 1;
                    int lim2 = lim1 + chunkSize - 1;

                    ArrayList<GenericFile> chunkListSnptestOutFile = new ArrayList<>();
                    ArrayList<GenericFile> chunkListSnptestLogFile = new ArrayList<>();
                    ArrayList<GenericFile> chunkListSummaryFile = new ArrayList<>();

                    for (int k = 0; k < total_chunks; k++) {
                        // Now we have to create the impute files for this iteration
                        String tmpSnptestOutFileName = "chr_" + chromo + "_" + testTypeName + "_" + rPanel + "_" + lim1 + "_" + lim2
                                + "_snptest.out.gz";
                        // String tmpSnptestOutFile = tmpChrDir + File.separator + tmpSnptestOutFileName;
                        GenericFile myChunkListSnptestOutFile = new GenericFile(tmpChrDir, tmpSnptestOutFileName, "uncompressed", "none");

                        chunkListSnptestOutFile.add(myChunkListSnptestOutFile);

                        String tmpSnptestLogFileName = "chr_" + chromo + "_" + testTypeName + "_" + rPanel + "_" + lim1 + "_" + lim2
                                + "_snptest.log";
                        // String tmpSnptestLogFile = tmpChrDir + File.separator + tmpSnptestLogFileName;
                        GenericFile myChunkListSnptestLogFile = new GenericFile(tmpChrDir, tmpSnptestLogFileName, "uncompressed", "none");
                        chunkListSnptestLogFile.add(myChunkListSnptestLogFile);

                        String tmpSummaryFileName = "chr_" + chromo + "_" + testTypeName + "_" + rPanel + "_" + lim1 + "_" + lim2
                                + "_summary.txt.gz";
                        // String tmpSummaryFile = tmpChrDir + File.separator + tmpSummaryFileName;
                        GenericFile myChunkListSummaryFile = new GenericFile(tmpChrDir, tmpSummaryFileName, "uncompressed", "none");
                        chunkListSummaryFile.add(myChunkListSummaryFile);

                        lim1 = lim1 + chunkSize;
                        lim2 = lim2 + chunkSize;
                    }

                    chromoListSnptestOutFile.add(chunkListSnptestOutFile);
                    chromoListSnptestLogFile.add(chunkListSnptestLogFile);
                    chromoListSummaryFile.add(chunkListSummaryFile);
                } // End for chromosomes

                rpanelListOutDir.add(chromoListOutDir);

                rpanelListSnptestOutFile.add(chromoListSnptestOutFile);
                rpanelListSnptestLogFile.add(chromoListSnptestLogFile);
                rpanelListSummaryFile.add(chromoListSummaryFile);

            } // End of for panels
            snptestOutFile.add(rpanelListSnptestOutFile);
            snptestLogFile.add(rpanelListSnptestLogFile);
            summaryFile.add(rpanelListSummaryFile);

            outDir.add(rpanelListOutDir);
        } // End for test types

    }

    /**
     * Method to access outDir information
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @return
     */
    public String getAssocOutDir(int testTypeIndex, int rPanelIndex, int chromo) {
        validateChromo(chromo, 1, 1);

        // The offset is because the array starts in position 0
        int chrIndex = chromo - startChr;
        return outDir.get(testTypeIndex).get(rPanelIndex).get(chrIndex);
    }

    /**
     * Method to access snptestOutFileName
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getSnptestOutFileName(int testTypeIndex, int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        // The offset is because the array starts in position 0
        int chrIndex = chromo - startChr;
        int chunckIndex = lim1 / chunkSize;

        return snptestOutFile.get(testTypeIndex).get(rPanelIndex).get(chrIndex).get(chunckIndex).getName();
    }

    /**
     * Method to access snptestOutFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getSnptestOutFile(int testTypeIndex, int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        // The offset is because the array starts in position 0
        int chrIndex = chromo - startChr;
        int chunckIndex = lim1 / chunkSize;

        return snptestOutFile.get(testTypeIndex).get(rPanelIndex).get(chrIndex).get(chunckIndex).getFullName();
    }

    /**
     * Method to set the finalStatus of snptestOutFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @param finalStatus
     */
    public void setSnptestOutFileFinalStatus(int testTypeIndex, int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize,
            String finalStatus) {

        validateChromo(chromo, lim1, lim2);

        // The offset is because the array starts in position 0
        int chrIndex = chromo - startChr;
        int chunckIndex = lim1 / chunkSize;

        snptestOutFile.get(testTypeIndex).get(rPanelIndex).get(chrIndex).get(chunckIndex).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of snptestOutFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getSnptestOutFileFinalStatus(int testTypeIndex, int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        // The offset is because the array starts in position 0
        int chrIndex = chromo - startChr;
        int chunckIndex = lim1 / chunkSize;

        return snptestOutFile.get(testTypeIndex).get(rPanelIndex).get(chrIndex).get(chunckIndex).getFinalStatus();
    }

    /**
     * Method to access snptestLogFileName
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getSnptestLogFileName(int testTypeIndex, int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        // The offset is because the array starts in position 0
        int chrIndex = chromo - startChr;
        int chunckIndex = lim1 / chunkSize;

        return snptestLogFileName.get(testTypeIndex).get(rPanelIndex).get(chrIndex).get(chunckIndex).getName();
    }

    /**
     * Method to access snptestLogFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getSnptestLogFile(int testTypeIndex, int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        // The offset is because the array starts in position 0
        int chrIndex = chromo - startChr;
        int chunckIndex = lim1 / chunkSize;

        return snptestLogFile.get(testTypeIndex).get(rPanelIndex).get(chrIndex).get(chunckIndex).getFullName();
    }

    /**
     * Method to access snptestLogFileName
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getSummaryFileName(int testTypeIndex, int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        // The offset is because the array starts in position 0
        int chrIndex = chromo - startChr;
        int chunckIndex = lim1 / chunkSize;

        return summaryFile.get(testTypeIndex).get(rPanelIndex).get(chrIndex).get(chunckIndex).getName();
    }

    /**
     * Method to access snptestLogFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getSummaryFile(int testTypeIndex, int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        // The offset is because the array starts in position 0
        int chrIndex = chromo - startChr;
        int chunckIndex = lim1 / chunkSize;

        return summaryFile.get(testTypeIndex).get(rPanelIndex).get(chrIndex).get(chunckIndex).getFullName();
    }

    /**
     * Method to set the finalStatus of snptestLogFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @param finalStatus
     */
    public void setSummaryFileFinalStatus(int testTypeIndex, int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize,
            String finalStatus) {

        validateChromo(chromo, lim1, lim2);

        // The offset is because the array starts in position 0
        int chrIndex = chromo - startChr;
        int chunckIndex = lim1 / chunkSize;

        summaryFile.get(testTypeIndex).get(rPanelIndex).get(chrIndex).get(chunckIndex).setFinalStatus(finalStatus);
    }

    /**
     * Method to access the finalStatus of snptestLogFile
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     * @return
     */
    public String getSummaryFileFinalStatus(int testTypeIndex, int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        // The offset is because the array starts in position 0
        int chrIndex = chromo - startChr;
        int chunckIndex = lim1 / chunkSize;
        return summaryFile.get(testTypeIndex).get(rPanelIndex).get(chrIndex).get(chunckIndex).getFinalStatus();
    }

    /**
     * Method to print Association files information
     * 
     * @param testTypeIndex
     * @param rPanelIndex
     * @param chromo
     * @param lim1
     * @param lim2
     * @param chunkSize
     */
    public void printAssocFiles(int testTypeIndex, int rPanelIndex, int chromo, int lim1, int lim2, int chunkSize) {
        validateChromo(chromo, lim1, lim2);

        // The offset is because the array starts in position 0
        int i = chromo - startChr;
        int indexLow = lim1 / chunkSize;
        int indexHigh = lim2 / chunkSize;

        for (int j = indexLow; j < indexHigh; j++) {
            System.out.println("-------------------------------------------------");
            System.out.println("Assoc files information for the chromosome " + chromo);
            System.out.println("outDir                  : " + outDir.get(testTypeIndex).get(rPanelIndex).get(i));
            System.out.println("snptestOutFile    : " + snptestOutFile.get(testTypeIndex).get(rPanelIndex).get(i).get(indexLow));
            System.out.println("snptestLogFile    : " + snptestLogFile.get(testTypeIndex).get(rPanelIndex).get(i).get(indexLow));
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
            System.err.println("[AssocFiles] Error, chromosome " + chromo + " does not exist");
            System.exit(1);
        }

        if (lim1 < 1 || lim2 > ChromoInfo.MAX_CHROMOSOME_LIMIT) {
            System.err.println("[AssocFiles] Error, Chunk " + lim1 + "_" + lim2 + " does not exist for chromosome " + chromo);
            System.exit(1);
        }
    }

}
