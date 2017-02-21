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
 *  Last update: $LastChangedDate: 2015-02-20 16:41:09 +0100 (Fri, 20 Feb 2015) $
 *  Revision Number: $Revision: 25 $
 *  Last revision  : $LastChangedRevision: 25 $
 *  Written by     : Friman Sanchez C.
 *                 : friman.sanchez@gmail.com
 *  Modified by    :
 *                
 *  Guidance web page: http://cg.bsc.es/guidance/
 */

package guidance;

import guidance.exceptions.GuidanceTaskException;
import guidance.readers.FilteredByAllReader;
import guidance.readers.FilteredByAllXReader;
import guidance.readers.FullPhenomeFileReader;
import guidance.readers.ImputeFileReader;
import guidance.readers.MergeTwoChunksReader;
import guidance.readers.PanelReader;
import guidance.readers.PartialPhenomeFileReader;
import guidance.readers.TopHitsReader;
import guidance.readersWriters.CombineCondensedFilesReaderWriter;
import guidance.readersWriters.CreateListOfExcludedSnpsReaderWriter;
import guidance.readersWriters.CreateRsIdListReaderWriter;
import guidance.readersWriters.FilterByAllReaderWriter;
import guidance.readersWriters.FilterByInfoReaderWriter;
import guidance.readersWriters.GenerateTopHitsAllReaderWriter;
import guidance.readersWriters.JointCondensedFilesReaderWriter;
import guidance.readersWriters.JointFilteredByAllFilesReaderWriter;
import guidance.readersWriters.PostFilterHaplotypesReaderWriter;
import guidance.utils.ChromoInfo;
import guidance.utils.GZFilesUtils;
import guidance.utils.HashUtils;
import guidance.writers.CollectSummaryWriter;
import guidance.writers.CombinePanelsComplexWriter;
import guidance.writers.PhenoMatrixWriter;
import guidance.writers.MergeTwoChunksWriter;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Hashtable;
import java.util.zip.GZIPInputStream;


public class GuidanceImpl {

    // Debug
    private static final boolean DEBUG = true;

    // Headers
    public static final String HEADER_MIXED = "chr	position	rs_id_all	info_all	certainty_all	alleleA	alleleB	index	average_maximum_posterior_call	info	cohort_1_AA	cohort_1_AB	cohort_1_BB	cohort_1_NULL	all_AA	all_AB	all_BB	all_NULL	all_total	cases_AA	cases_AB	cases_BB	cases_NULL	cases_total	controls_AA	controls_AB	controls_BB	controls_NULL	controls_total	all_maf	cases_maf	controls_maf	missing_data_proportion	cohort_1_hwe	cases_hwe	controls_hwe	het_OR	het_OR_lower	het_OR_upper	hom_OR	hom_OR_lower	hom_OR_upper	all_OR	all_OR_lower	all_OR_upper	frequentist_add_pvalue	frequentist_add_info	frequentist_add_beta_1	frequentist_add_se_1	frequentist_dom_pvalue	frequentist_dom_info	frequentist_dom_beta_1	frequentist_dom_se_1	frequentist_rec_pvalue	frequentist_rec_info	frequentist_rec_beta_1	frequentist_rec_se_1	frequentist_gen_pvalue	frequentist_gen_info	frequentist_gen_beta_1	frequentist_gen_se_1	frequentist_gen_beta_2	frequentist_gen_se_2	frequentist_het_pvalue	frequentist_het_info	frequentist_het_beta_1	frequentist_het_se_1	comment";
    public static final String HEADER_MIXED_X = "chr	position	rs_id_all	info_all	certainty_all	alleleA	alleleB	all_A	all_B	all_AA	all_AB	all_BB	all_NULL	all_total	all_maf	all_info	all_impute_info	cases_A	cases_B	cases_AA	cases_AB	cases_BB	cases_NULL	cases_total	cases_maf	cases_info	cases_impute_info	controls_A	controls_B	controls_AA	controls_AB	controls_BB	controls_NULL	controls_total	controls_maf	controls_info	controls_impute_info	sex=1_A	sex=1_B	sex=1_AA	sex=1_AB	sex=1_BB	sex=1_NULL	sex=1_total	sex=1_maf	sex=1_info	sex=1_impute_info	sex=2_A	sex=2_B	sex=2_AA	sex=2_AB	sex=2_BB	sex=2_NULL	sex=2_total	sex=2_maf	sex=2_info	sex=2_impute_info	frequentist_add_null_ll	frequentist_add_alternative_ll	frequentist_add_beta_1:genotype/sex=1	frequentist_add_beta_2:genotype/sex=2	frequentist_add_se_1:genotype/sex=1	frequentist_add_se_2:genotype/sex=2	frequentist_add_degrees_of_freedom	frequentist_add_pvalue	comment";

    // Constants
    public static final String FLAG_YES = "YES";


    /**
     * Method to create a file that contains the SNPs positions of the input genFile
     * 
     * @param genOrBimFile
     * @param exclCgatFlag
     * @param pairsFile
     * @param inputFormat
     * @param cmdToStore
     * 
     * @throws GuidanceTaskException
     */
    public static void createRsIdList(String genOrBimFile, String exclCgatFlag, String pairsFile, String inputFormat)
            throws GuidanceTaskException {

        if (DEBUG) {
            System.out.println("\n");
            System.out.println("[DEBUG] Running createRsIdList with parameters:");
            System.out.println("[DEBUG] \t- Input genOrBimFile : " + genOrBimFile);
            System.out.println("[DEBUG] \t- Input exclCgatFlag : " + exclCgatFlag);
            System.out.println("[DEBUG] \t- Output pairsFile   : " + pairsFile);
            System.out.println("[DEBUG] \t- InputFormat        : " + inputFormat);
            System.out.println("\n");
        }

        // Timer
        long startTime = System.currentTimeMillis();

        // Check if the file is gzip or not.
        // This is done by checking the magic number of gzip files, which is 0x1f8b (the first two bytes)
        long n;
        try (RandomAccessFile raf = new RandomAccessFile(new File(genOrBimFile), "r")) {
            n = raf.readInt();
            n = n & 0xFFFF0000;
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot open gen or bim file", ioe);
        }

        boolean isFileGz;
        if (n == 0x1f8b0000) {
            System.out.println(
                    "[CreateRsIdList] It seems the file " + genOrBimFile + " is a gzip file. Magic Number is " + String.format("%x", n));
            isFileGz = true;
        } else {
            isFileGz = false;
        }

        // Process
        CreateRsIdListReaderWriter readerWriter = new CreateRsIdListReaderWriter(genOrBimFile, pairsFile, isFileGz, false);
        try {
            readerWriter.execute(exclCgatFlag, inputFormat);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot process pairs file", gte);
        }

        // Timer
        long stopTime = System.currentTimeMillis();
        long elapsedTime = (stopTime - startTime) / 1_000;
        if (DEBUG) {
            System.out.println("\n[DEBUG] createRsIdList StartTime: " + startTime);
            System.out.println("\n[DEBUG] createRsIdList endTime: " + stopTime);
            System.out.println("\n[DEBUG] createRsIdList elapsedTime: " + elapsedTime + " seconds");
            System.out.println("\n[DEBUG] Finished execution of createRsIdList.");
        }
    }

    /**
     * Method to execute createListOfExcludedSnps
     * 
     * @param shapeitHapsFile
     * @param excludedSnpsFile
     * @param exclCgatFlag
     * @param exclSVFlag
     * 
     * @throws GuidanceTaskException
     */
    public static void createListOfExcludedSnps(String shapeitHapsFile, String excludedSnpsFile, String exclCgatFlag, String exclSVFlag)
            throws GuidanceTaskException {

        if (DEBUG) {
            System.out.println("\n[DEBUG] Running createListOfExcludedSnps method:");
            System.out.println("[DEBUG] \t- Input shapeitHapsFile   : " + shapeitHapsFile);
            System.out.println("[DEBUG] \t- Output excludedSnpsFile : " + excludedSnpsFile);
            System.out.println("[DEBUG] \t- Input exclCgatFlag      : " + exclCgatFlag);
            System.out.println("[DEBUG] \t- Input exclSVFlag        : " + exclSVFlag);
            System.out.println("\n");
        }

        // Timer
        long startTime = System.currentTimeMillis();

        // We have to check the haplotypes file and extract tha allele information. For the format file used by shapeit,
        // the indices for alleles are:
        final int rsIdIndex = 1;
        final int posIndex = 2;
        final int a1Index = 3;
        final int a2Index = 4;
        // And the column separator is:
        final String separator = " ";

        // Process
        CreateListOfExcludedSnpsReaderWriter readerWriter = new CreateListOfExcludedSnpsReaderWriter(shapeitHapsFile, excludedSnpsFile,
                true, false);
        try {
            readerWriter.execute(exclCgatFlag, exclSVFlag, rsIdIndex, posIndex, a1Index, a2Index, separator);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot process A file", gte);
        }

        // Timer
        long stopTime = System.currentTimeMillis();
        long elapsedTime = (stopTime - startTime) / 1_000;

        if (DEBUG) {
            System.out.println("\n[DEBUG] createListOfExcludedSnps startTime: " + startTime);
            System.out.println("\n[DEBUG] createListOfExcludedSnps endTime: " + stopTime);
            System.out.println("\n[DEBUG] createListOfExcludedSnps elapsedTime: " + elapsedTime + " seconds");
            System.out.println("\n[DEBUG] Finished execution of createListOfExcludedSnps.");
        }
    }

    /**
     * Actions to do after filter haplotypes (decompress)
     * 
     * @param filteredHapsFile
     * @param listOfSnpsFile
     * 
     * @throws GuidanceTaskException
     */
    public static void postFilterHaplotypes(String filteredHapsFile, String listOfSnpsFile) throws GuidanceTaskException {
        // Now we have to create the list of snps and write them into the output file.
        if (DEBUG) {
            System.out.println("\n");
            System.out.println("[DEBUG] Running postFilterHaplotypes with parameters:");
            System.out.println("[DEBUG] \t- Filtered Haps File     : " + filteredHapsFile);
            System.out.println("[DEBUG] \t- List of Snps File      : " + listOfSnpsFile);
            System.out.println("\n");
        }

        // Timer
        long startTime = System.currentTimeMillis();

        // Process
        PostFilterHaplotypesReaderWriter readerWriter = new PostFilterHaplotypesReaderWriter(filteredHapsFile, listOfSnpsFile, true, false);
        try {
            readerWriter.execute();
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot process FilterByInfo file", gte);
        }

        // Timer
        long stopTime = System.currentTimeMillis();
        long elapsedTime = (stopTime - startTime) / 1_000;

        if (DEBUG) {
            System.out.println("\n[DEBUG] postFilterHaplotypes startTime: " + startTime);
            System.out.println("\n[DEBUG] postFilterHaplotypes endTime: " + stopTime);
            System.out.println("\n[DEBUG] postFilterHaplotypes elapsedTime: " + elapsedTime + " seconds");
            System.out.println("\n[DEBUG] Finished execution of postFilterHaplotypes");
        }
    }

    /**
     * Method to filter by info
     * 
     * @param imputeFileInfo
     * @param filteredFile
     * @param threshold
     * 
     * @throws GuidanceTaskException
     */
    public static void filterByInfo(String imputeFileInfo, String filteredFile, String threshold) throws GuidanceTaskException {
        if (DEBUG) {
            System.out.println("\n");
            System.out.println("[DEBUG] Running filterByInfo with parameters:");
            System.out.println("[DEBUG] \t- Input inputeFileInfo   : " + imputeFileInfo);
            System.out.println("[DEBUG] \t- Output filteredFile    : " + filteredFile);
            System.out.println("[DEBUG] \t- Input threshold        : " + threshold);
            System.out.println("\n");
        }

        // Timer
        long startTime = System.currentTimeMillis();

        // The position of info and rsId values in the imputeFileInfo
        int infoIndex = 6;
        int rsIdIndex = 1;
        // Convert threshold string into thresholdDouble
        Double thresholdDouble = Double.parseDouble(threshold); // store info value in Double format

        // Process
        FilterByInfoReaderWriter readerWriter = new FilterByInfoReaderWriter(imputeFileInfo, filteredFile, false, false);
        try {
            readerWriter.execute(infoIndex, rsIdIndex, thresholdDouble);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot process FilterByInfo file", gte);
        }

        // Timer
        long stopTime = System.currentTimeMillis();
        long elapsedTime = (stopTime - startTime) / 1_000;

        if (DEBUG) {
            System.out.println("\n[DEBUG] filterByInfo startTime: " + startTime);
            System.out.println("\n[DEBUG] filterByInfo endTime: " + stopTime);
            System.out.println("\n[DEBUG] filterByInfo elapsedTime: " + elapsedTime + " seconds");
            System.out.println("\n[DEBUG] Finished execution of filterByInfo");
        }
    }

    /**
     * Method to filter by all
     * 
     * @param inputFile
     * @param outputFile
     * @param outputCondensedFile
     * @param mafThresholdS
     * @param infoThresholdS
     * @param hweCohortThresholdS
     * @param hweCasesThresholdS
     * @param hweControlsThresholdS
     * 
     * @throws GuidanceTaskException
     */
    public static void filterByAll(String inputFile, String outputFile, String outputCondensedFile, String mafThresholdS,
            String infoThresholdS, String hweCohortThresholdS, String hweCasesThresholdS, String hweControlsThresholdS)
            throws GuidanceTaskException {

        if (DEBUG) {
            System.out.println("\n");
            System.out.println("[DEBUG] Running filterByAll with parameters:");
            System.out.println("[DEBUG] \t- Input summaryFile             : " + inputFile);
            System.out.println("[DEBUG] \t- Output outputFile             : " + outputFile);
            System.out.println("[DEBUG] \t- Output outputCondensedFile    : " + outputCondensedFile);
            System.out.println("[DEBUG] \t- Input maf threshold           : " + mafThresholdS);
            System.out.println("[DEBUG] \t- Input info threshold          : " + infoThresholdS);
            System.out.println("[DEBUG] \t- Input hwe cohort threshold    : " + hweCohortThresholdS);
            System.out.println("[DEBUG] \t- Input hwe controls threshold  : " + hweCasesThresholdS);
            System.out.println("[DEBUG] \t- Input hwe cases threshold     : " + hweControlsThresholdS);
            System.out.println("\n");
        }

        // Timer
        long startTime = System.currentTimeMillis();

        // Convert threshold string into thresholdDouble
        Double mafThreshold = Double.parseDouble(mafThresholdS);
        Double infoThreshold = Double.parseDouble(infoThresholdS);
        Double hweCohortThreshold = Double.parseDouble(hweCohortThresholdS);
        Double hweCasesThreshold = Double.parseDouble(hweCasesThresholdS);
        Double hweControlsThreshold = Double.parseDouble(hweControlsThresholdS);

        // Process
        FilterByAllReaderWriter readerWriter = new FilterByAllReaderWriter(inputFile, outputFile, true, false);
        try {
            readerWriter.execute(outputCondensedFile, mafThreshold, infoThreshold, hweCohortThreshold, hweCasesThreshold,
                    hweControlsThreshold);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot process A file", gte);
        }

        // Then, we create the gz file and rename it
        try {
            GZFilesUtils.gzipFile(outputFile, outputFile + ".gz");
            File fc = new File(outputFile);
            File fGz = new File(outputFile + ".gz");
            GZFilesUtils.copyFile(fGz, fc);
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot zip output file", ioe);
        }
        try {
            GZFilesUtils.gzipFile(outputCondensedFile, outputCondensedFile + ".gz");
            File fc = new File(outputCondensedFile);
            File fGz = new File(outputCondensedFile + ".gz");
            GZFilesUtils.copyFile(fGz, fc);
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot zip output condensed file", ioe);
        }

        // Timer
        long stopTime = System.currentTimeMillis();
        long elapsedTime = (stopTime - startTime) / 1_000;

        if (DEBUG) {
            System.out.println("\n[DEBUG] filterByAll startTime: " + startTime);
            System.out.println("\n[DEBUG] filterByAll endTime: " + stopTime);
            System.out.println("\n[DEBUG] filterByAll elapsedTime: " + elapsedTime + " seconds");
            System.out.println("\n[DEBUG] Finished execution of filterByAll");
        }
    }

    /**
     * Method to joint filtered by all files
     * 
     * @param filteredByAllA
     * @param filteredByAllB
     * @param filteredByAllC
     * @param rpanelName
     * @param rpanelFlag
     * 
     * @throws GuidanceTaskException
     */
    public static void jointFilteredByAllFiles(String filteredByAllA, String filteredByAllB, String filteredByAllC, String rpanelName,
            String rpanelFlag) throws GuidanceTaskException {

        if (DEBUG) {
            System.out.println("\n[DEBUG] Running jointFilteredByAllFiles with parameters:");
            System.out.println("[DEBUG] \t- Input filteredByAllA          : " + filteredByAllA);
            System.out.println("[DEBUG] \t- Input filteredByAllB          : " + filteredByAllB);
            System.out.println("[DEBUG] \t- Output filteredByAllC         : " + filteredByAllC);
            System.out.println("\n");

        }

        // Timer
        long startTime = System.currentTimeMillis();

        // Process A input
        JointFilteredByAllFilesReaderWriter readerWriter = new JointFilteredByAllFilesReaderWriter(filteredByAllA, filteredByAllC, true,
                false);
        try {
            readerWriter.execute(rpanelName);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot process A file", gte);
        }

        // Do the same with the filteredByAllB file if this file is different to the filteredByAllA file
        // WARN: The only way that filteredByAllB = filteredByAllA is when there is only one chromosome to process
        // In that case, the main program sends the same file as filteredByAllA and filteredByAllB
        // Process B input
        if (!filteredByAllA.equals(filteredByAllB)) {
            readerWriter = new JointFilteredByAllFilesReaderWriter(filteredByAllB, filteredByAllC, true, true);
            try {
                readerWriter.execute(rpanelName);
            } catch (GuidanceTaskException gte) {
                throw new GuidanceTaskException("[ERROR] Cannot process B file", gte);
            }
        }

        // Then, we create the gz file and rename it
        try {
            GZFilesUtils.gzipFile(filteredByAllC, filteredByAllC + ".gz");
            File fc = new File(filteredByAllC);
            File fGz = new File(filteredByAllC + ".gz");
            GZFilesUtils.copyFile(fGz, fc);
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot zip filteredByAllC file", ioe);
        }

        // Timer
        long stopTime = System.currentTimeMillis();
        long elapsedTime = (stopTime - startTime) / 1_000;

        if (DEBUG) {
            System.out.println("\n[DEBUG] jointFilteredByAllFiles startTime: " + startTime);
            System.out.println("\n[DEBUG] jointFilteredByAllFiles endTime: " + stopTime);
            System.out.println("\n[DEBUG] jointFilteredByAllFiles elapsedTime: " + elapsedTime + " seconds");
            System.out.println("\n[DEBUG] Finished execution of jointFilteredByAllFiles");
        }

    }

    /**
     * Method to joint condensed files
     * 
     * @param inputAFile
     * @param inputBFile
     * @param outputFile
     * 
     * @throws GuidanceTaskException
     */
    public static void jointCondensedFiles(String inputAFile, String inputBFile, String outputFile) throws GuidanceTaskException {
        if (DEBUG) {
            System.out.println("\n");
            System.out.println("[DEBUG] Running jointCondensedFiles with parameters:");
            System.out.println("[DEBUG] \t- InputAFile                    : " + inputAFile);
            System.out.println("[DEBUG] \t- InputBFile                    : " + inputBFile);
            System.out.println("[DEBUG] \t- Output outputCondensedFile    : " + outputFile);
            System.out.println("\n");
        }

        // Timer
        long startTime = System.currentTimeMillis();

        // Process A input
        JointCondensedFilesReaderWriter readerWriter = new JointCondensedFilesReaderWriter(inputAFile, outputFile, true, false);
        try {
            readerWriter.execute();
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot process A file", gte);
        }

        // Do the same with the filteredByAllB file if this file is different to the filteredByAllA file
        // WARN: The only way that filteredByAllB = filteredByAllA is when there is only one chromosome to process
        // In that case, the main program sends the same file as filteredByAllA and filteredByAllB
        // Process B input
        if (!inputAFile.equals(inputBFile)) {
            readerWriter = new JointCondensedFilesReaderWriter(inputBFile, outputFile, true, true);
            try {
                readerWriter.execute();
            } catch (GuidanceTaskException gte) {
                throw new GuidanceTaskException("[ERROR] Cannot process B file", gte);
            }
        }

        // Then, we create the gz file and rename it
        try {
            GZFilesUtils.gzipFile(outputFile, outputFile + ".gz");
            File fc = new File(outputFile);
            File fGz = new File(outputFile + ".gz");
            GZFilesUtils.copyFile(fGz, fc);
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot zip filteredByAllC file", ioe);
        }

        // Timer
        long stopTime = System.currentTimeMillis();
        long elapsedTime = (stopTime - startTime) / 1_000;
        if (DEBUG) {
            System.out.println("\n[DEBUG] jointCondensedFiles startTime: " + startTime);
            System.out.println("\n[DEBUG] jointCondensedFiles endTime: " + stopTime);
            System.out.println("\n[DEBUG] jointCondensedFiles elapsedTime: " + elapsedTime + " seconds");
            System.out.println("\n[DEBUG] Finished execution of jointCondensedFiles");
        }
    }

    /**
     * Method to combine panels complex
     * 
     * @param resultsPanelA
     * @param resultsPanelB
     * @param resultsPanelC
     * @param chromoStart
     * @param chromoEnd
     * 
     * @throws GuidanceTaskException
     */
    public static void combinePanelsComplex(String resultsPanelA, String resultsPanelB, String resultsPanelC, String chromoStart,
            String chromoEnd) throws GuidanceTaskException {

        if (DEBUG) {
            System.out.println("\n");
            System.out.println("[DEBUG] Running combinePanelsComplex with parameters:");
            System.out.println("[DEBUG] \t- resultsPanelA             : " + resultsPanelA);
            System.out.println("[DEBUG] \t- resultsPanelB             : " + resultsPanelB);
            System.out.println("[DEBUG] \t- resultsPanelC             : " + resultsPanelC);
            System.out.println("[DEBUG] \t- chromoStart               : " + chromoStart);
            System.out.println("[DEBUG] \t- chromoEnd                 : " + chromoEnd);
            System.out.println("\n");
        }

        // Timer
        long startTime = System.currentTimeMillis();
        int chrStart = Integer.parseInt(chromoStart);
        int chrEnd = Integer.parseInt(chromoEnd);

        // First, we uncompress the input files
        try {
            GZFilesUtils.gunzipFile(resultsPanelA, resultsPanelA + ".temp");
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot unzip panel A", ioe);
        }
        try {
            GZFilesUtils.gunzipFile(resultsPanelB, resultsPanelB + ".temp");
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot unzip panel B", ioe);
        }

        // We read each line of the resultsPanelA and put them into the String
        // IMPORTANT: In that case we sort by position and not by position_rsID. So, maybe we
        // are going to lose some SNPs...
        String separator = "\t";
        for (int chromo = chrStart; chromo <= chrEnd; chromo++) {
            String chromoS = Integer.toString(chromo);

            // Create the first treeMap for the chromo
            Hashtable<String, Integer> phenomeAHashTableIndex = new Hashtable<>();
            Hashtable<Integer, String> phenomeAHashTableIndexReversed = new Hashtable<>();
            TreeMap<String, ArrayList<String>> fileATreeMap = null;
            PanelReader readerA = new PanelReader(resultsPanelA + ".temp", false);
            try {
                fileATreeMap = readerA.read(separator, chromoS, phenomeAHashTableIndex, phenomeAHashTableIndexReversed);
            } catch (GuidanceTaskException gte) {
                throw new GuidanceTaskException("[ERROR] Cannot process fileA", gte);
            }

            // Create the second treeMap for the chromo
            Hashtable<String, Integer> phenomeBHashTableIndex = new Hashtable<>();
            Hashtable<Integer, String> phenomeBHashTableIndexReversed = new Hashtable<>();
            TreeMap<String, ArrayList<String>> fileBTreeMap = null;
            PanelReader readerB = new PanelReader(resultsPanelB + ".temp", false);
            try {
                fileBTreeMap = readerB.read(separator, chromoS, phenomeBHashTableIndex, phenomeBHashTableIndexReversed);
            } catch (GuidanceTaskException gte) {
                throw new GuidanceTaskException("[ERROR] Cannot process fileB", gte);
            }

            // Obtain the indexes
            int chrIdx = phenomeAHashTableIndex.get("chr");
            int posIdx = phenomeAHashTableIndex.get("position");
            int a1Idx = phenomeAHashTableIndex.get("alleleA");
            int a2Idx = phenomeAHashTableIndex.get("alleleB");
            int infoIdx = phenomeAHashTableIndex.get("info_all");

            // Merge the results
            TreeMap<String, ArrayList<String>> fileCTreeMap = new TreeMap<>();
            // We first iterate the fileTreeMapA
            Iterator<Entry<String, ArrayList<String>>> iter = fileATreeMap.entrySet().iterator();
            while (iter.hasNext()) {
                // key=value separator this by Map.Entry to get key and value
                Entry<String, ArrayList<String>> m = iter.next();
                String positionA1A2Chr = m.getKey();
                ArrayList<String> lineA = m.getValue();

                String[] splittedA = lineA.get(0).split("\t");
                Double infoA = Double.parseDouble(splittedA[infoIdx]);
                String posAllelesReverse = splittedA[posIdx] + "_" + getAllele(splittedA[a1Idx], splittedA[a2Idx], "reverse") + "_"
                        + splittedA[chrIdx];
                String posAllelesComplement = splittedA[posIdx] + "_" + getAllele(splittedA[a1Idx], splittedA[a2Idx], "complement") + "_"
                        + splittedA[chrIdx];
                String posAllelesComplementAndReverse = splittedA[posIdx] + "_"
                        + getAllele(splittedA[a1Idx], splittedA[a2Idx], "complementAndReverse") + "_" + splittedA[chrIdx];

                // System.out.println("[combinePanelsComplex] " + positionA1A2Chr + " " + posAllelesEqual + " " +
                // posAllelesReverse + " " + posAllelesComplement + " " + posAllelesComplementAndReverse);

                // The same: position, a1 and a2?
                if (fileBTreeMap.containsKey(positionA1A2Chr)) {
                    // If the fileTreeMapB contains this positionA1A2Chr combination, then we have to choose
                    // the ones that has a better info (that is the ones with greater info).
                    ArrayList<String> lineB = fileBTreeMap.get(positionA1A2Chr);
                    String[] splittedB = lineB.get(0).split("\t");
                    Double infoB = Double.parseDouble(splittedB[infoIdx]);
                    // Then we have to choose between A o B.
                    if (infoA >= infoB) {
                        fileCTreeMap.put(positionA1A2Chr, lineA);
                    } else {
                        fileCTreeMap.put(positionA1A2Chr, lineB);
                    }
                    // System.out.println("WOW alelos iguales: " + positionA1A2Chr);

                    // Now we remove this value from the fileTreeMapB
                    fileBTreeMap.remove(positionA1A2Chr);
                } else if (fileBTreeMap.containsKey(posAllelesReverse)) {
                    // If the fileTreeMapB contains this posAllelesReverse, then we have to choose
                    // the ones that has a better info (that is the ones with greater info).
                    ArrayList<String> lineB = fileBTreeMap.get(posAllelesReverse);
                    String[] splittedB = lineB.get(0).split("\t");
                    Double infoB = Double.parseDouble(splittedB[infoIdx]);
                    // Then we have to choose between A and B.
                    if (infoA >= infoB) {
                        fileCTreeMap.put(positionA1A2Chr, lineA);
                    } else {
                        fileCTreeMap.put(posAllelesReverse, lineB);
                    }
                    // Now we remove this value from the fileTreeMapB
                    fileBTreeMap.remove(posAllelesReverse);
                    // System.out.println("WOW alelos reversos: " + positionA1A2Chr + " " + posAllelesReverse);
                } else if (fileBTreeMap.containsKey(posAllelesComplement)) {
                    // If the fileTreeMapB contains this posAllelesComplement, then we have to choose
                    // the ones that has a better info (that is the ones with greater info).
                    ArrayList<String> lineB = fileBTreeMap.get(posAllelesComplement);
                    String[] splittedB = lineB.get(0).split("\t");
                    Double infoB = Double.parseDouble(splittedB[infoIdx]);
                    // Then we have to choose between A o B.
                    if (infoA >= infoB) {
                        fileCTreeMap.put(positionA1A2Chr, lineA);
                    } else {
                        fileCTreeMap.put(posAllelesComplement, lineB);
                    }
                    // Now we remove this value from the fileTreeMapB
                    fileBTreeMap.remove(posAllelesComplement);
                    // System.out.println("WOW alelos complementarios: " + positionA1A2Chr + " " +
                    // posAllelesComplement);
                } else if (fileBTreeMap.containsKey(posAllelesComplementAndReverse)) {
                    // If the fileTreeMapB contains this posAllelesComplement, then we have to choose
                    // the ones that has a better info (that is the ones with greater info).
                    ArrayList<String> lineB = fileBTreeMap.get(posAllelesComplementAndReverse);
                    String[] splittedB = lineB.get(0).split("\t");
                    Double infoB = Double.parseDouble(splittedB[infoIdx]);
                    // Then we have to choose between A o B.
                    if (infoA >= infoB) {
                        fileCTreeMap.put(positionA1A2Chr, lineA);
                    } else {
                        fileCTreeMap.put(posAllelesComplementAndReverse, lineB);
                    }
                    // Now we remove this value from the fileTreeMapB
                    fileBTreeMap.remove(posAllelesComplementAndReverse);
                    // System.out.println("WOW alelos complementariosYreversos: " + positionA1A2Chr + " " +
                    // posAllelesComplementAndReverse);
                } else {
                    // Else means that fileTreeMapB does not contain this SNP or any of its variants.
                    // Therefore, we keep the one in fileTreeMapA
                    fileCTreeMap.put(positionA1A2Chr, lineA);
                    // System.out.println("WOW fileTreeMapB does not contain this SNP: " + positionA1A2Chr);
                }
                // contador++;
            }

            // Now we have to put in fileTreeMapC the rest of values that remain in fileTreeMapB.
            // We iterate the fileTreeMapB (the rest of the...)
            iter = fileBTreeMap.entrySet().iterator();
            while (iter.hasNext()) {
                // key=value separator this by Map.Entry to get key and value
                Entry<String, ArrayList<String>> m = iter.next();
                // getKey is used to get key of Map
                String positionA1A2Chr = m.getKey();
                ArrayList<String> lineB = m.getValue();
                // Then we have to store the value in fileTreeMapC
                fileCTreeMap.put(positionA1A2Chr, lineB);
            }

            // System.out.println("\n[DEBUG] We have processed the chromosome " + chromoS + ". contador " + contador);

            // Finally we put the fileTreeMapC into the outputFile
            CombinePanelsComplexWriter writer = new CombinePanelsComplexWriter(resultsPanelC);
            try {
                writer.write(chromo == chrStart, phenomeAHashTableIndexReversed, fileCTreeMap);
            } catch (GuidanceTaskException gte) {
                throw new GuidanceTaskException("[ERROR] Cannot write summary total reduce File", gte);
            }

        } // End of for per Chromo

        // Then, we create the gz file and rename it
        try {
            GZFilesUtils.gzipFile(resultsPanelC, resultsPanelC + ".gz");
            File fc = new File(resultsPanelC);
            File fGz = new File(resultsPanelC + ".gz");
            GZFilesUtils.copyFile(fGz, fc);
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot zip results panel C file", ioe);
        }

        System.out.println("\n[DEBUG] Finished all chromosomes");

        // Timer
        long stopTime = System.currentTimeMillis();
        long elapsedTime = (stopTime - startTime) / 1_000;

        if (DEBUG) {
            System.out.println("\n[DEBUG] combinePanelsComplex startTime: " + startTime);
            System.out.println("\n[DEBUG] combinePanelsComplex endTime: " + stopTime);
            System.out.println("\n[DEBUG] combinePanelsComplex elapsedTime: " + elapsedTime + " seconds");
            System.out.println("\n[DEBUG] Finished execution of combinePanelsComplex");
        }

    }

    /**
     * Method to combine condensed files
     * 
     * @param filteredA
     * @param filteredX
     * @param combinedCondensedFile
     * @param mafThresholdS
     * @param infoThresholdS
     * @param hweCohortThresholdS
     * @param hweCasesThresholdS
     * @param hweControlsThresholdS
     * 
     * @throws GuidanceTaskException
     */
    public static void combineCondensedFiles(String filteredA, String filteredX, String combinedCondensedFile, String mafThresholdS,
            String infoThresholdS, String hweCohortThresholdS, String hweCasesThresholdS, String hweControlsThresholdS)
            throws GuidanceTaskException {

        if (DEBUG) {
            System.out.println("\n");
            System.out.println("[DEBUG] Running combineCondensedFiles with parameters:");
            System.out.println("[DEBUG] \t- Input filteredA             : " + filteredA);
            System.out.println("[DEBUG] \t- Input filteredX             : " + filteredX);
            System.out.println("[DEBUG] \t- Output combinedCondensedFile : " + combinedCondensedFile);
            System.out.println("[DEBUG] \t- Input maf threshold           : " + mafThresholdS);
            System.out.println("[DEBUG] \t- Input info threshold          : " + infoThresholdS);
            System.out.println("[DEBUG] \t- Input hwe cohort threshold    : " + hweCohortThresholdS);
            System.out.println("[DEBUG] \t- Input hwe controls threshold  : " + hweCasesThresholdS);
            System.out.println("[DEBUG] \t- Input hwe cases threshold     : " + hweControlsThresholdS);
            System.out.println("\n");
        }

        // Timer
        long startTime = System.currentTimeMillis();

        // Convert threshold string into thresholdDouble
        Double mafThreshold = Double.parseDouble(mafThresholdS);
        Double infoThreshold = Double.parseDouble(infoThresholdS);
        Double hweCohortThreshold = Double.parseDouble(hweCohortThresholdS);
        Double hweCasesThreshold = Double.parseDouble(hweCasesThresholdS);
        Double hweControlsThreshold = Double.parseDouble(hweControlsThresholdS);

        // Process normal chromosomes
        CombineCondensedFilesReaderWriter readerWriter = new CombineCondensedFilesReaderWriter(filteredA, combinedCondensedFile, true,
                false);
        try {
            readerWriter.execute(mafThreshold, infoThreshold, hweCohortThreshold, hweCasesThreshold, hweControlsThreshold);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot process A file", gte);
        }

        // Now with crh 23
        // If filteredA != filteredX then there is chr23 file (filteredX, therefore we have to include it in the
        // results.
        // Other wise, there is nothing to do.
        if (!filteredA.equals(filteredX)) {
            readerWriter = new CombineCondensedFilesReaderWriter(filteredA, combinedCondensedFile, true, true);
            try {
                readerWriter.execute(mafThreshold, infoThreshold, hweCohortThreshold, hweCasesThreshold, hweControlsThreshold);
            } catch (GuidanceTaskException gte) {
                throw new GuidanceTaskException("[ERROR] Cannot process B file", gte);
            }
        }

        // Zip and move the file
        try {
            GZFilesUtils.gzipFile(combinedCondensedFile, combinedCondensedFile + ".gz");
            File fc = new File(combinedCondensedFile);
            File fGz = new File(combinedCondensedFile + ".gz");
            GZFilesUtils.copyFile(fGz, fc);
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot zip combined condensed file", ioe);
        }

        // Timer
        long stopTime = System.currentTimeMillis();
        long elapsedTime = (stopTime - startTime) / 1_000;

        if (DEBUG) {
            System.out.println("\n[DEBUG] combineCondensedFiles startTime: " + startTime);
            System.out.println("\n[DEBUG] combineCondensedFiles endTime: " + stopTime);
            System.out.println("\n[DEBUG] combineCondensedFiles elapsedTime: " + elapsedTime + " seconds");
            System.out.println("\n[DEBUG] Finished execution of combinedCondensedFiles");
        }
    }

    /**
     * Method to generate all the top hits
     * 
     * @param resultsAFile
     * @param resultsBFile
     * @param outputTopHitFile
     * @param pvaThreshold
     * 
     * @throws GuidanceTaskException
     */
    public static void generateTopHitsAll(String resultsAFile, String resultsBFile, String outputTopHitFile, String pvaThreshold)
            throws GuidanceTaskException {

        if (DEBUG) {
            System.out.println("\n");
            System.out.println("[DEBUG] Running generateTopHits with parameters:");
            System.out.println("[DEBUG] \t- resultsAFile                : " + resultsAFile);
            System.out.println("[DEBUG] \t- resultsBFile                : " + resultsBFile);
            System.out.println("[DEBUG] \t- outputTopHitFile           : " + outputTopHitFile);
            System.out.println("[DEBUG] \t- pvaThreshold               : " + pvaThreshold);
            System.out.println("\n");
        }

        // Timer
        long startTime = System.currentTimeMillis();

        double pvaThres = Double.parseDouble(pvaThreshold);

        // Process A file
        GenerateTopHitsAllReaderWriter readerWriter = new GenerateTopHitsAllReaderWriter(resultsAFile, outputTopHitFile, true, false);
        try {
            readerWriter.execute(pvaThres);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot process A file", gte);
        }

        // Now we have to see if we have to include results for Chr23 that come in the second input file (resultsBFile)
        // The way to know whether we have results for chr23 is by checking that resultsAFile is equal to resultsBFile.
        // If they are equal, the we do not have chr23. Other wise we have to results for chr23 y resultsBFile and we
        // have
        // to include it in the outputTopHitFile
        if (!resultsAFile.equals(resultsBFile)) {
            readerWriter = new GenerateTopHitsAllReaderWriter(resultsAFile, outputTopHitFile, true, true);
            try {
                readerWriter.execute(pvaThres);
            } catch (GuidanceTaskException gte) {
                throw new GuidanceTaskException("[ERROR] Cannot process B file", gte);
            }
        }

        // Then, we create the gz file and rename it
        try {
            GZFilesUtils.gzipFile(outputTopHitFile, outputTopHitFile + ".gz");
            File fc = new File(outputTopHitFile);
            File fGz = new File(outputTopHitFile + ".gz");
            GZFilesUtils.copyFile(fGz, fc);
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot zip outputTopHit file", ioe);
        }

        // Timer
        long stopTime = System.currentTimeMillis();
        long elapsedTime = (stopTime - startTime) / 1_000;

        if (DEBUG) {
            System.out.println("\n[DEBUG] generateTopHits startTime:  " + startTime);
            System.out.println("\n[DEBUG] generateTopHits endTime:    " + stopTime);
            System.out.println("\n[DEBUG] generateTopHits elapsedTime: " + elapsedTime + " seconds");
            System.out.println("\n[DEBUG] Finished execution of generateTopHits");
        }
    }

    /**
     * Method to merge to chunks
     * 
     * @param reduceFileA
     * @param reduceFileB
     * @param reduceFileC
     * @param chrS
     * 
     * @throws GuidanceTaskException
     */
    public static void mergeTwoChunks(String reduceFileA, String reduceFileB, String reduceFileC, String chrS)
            throws GuidanceTaskException {

        if (DEBUG) {
            System.out.println("\n");
            System.out.println("[DEBUG] Running mergeTwoChunks with parameters:");
            System.out.println("[DEBUG] \t- Input reduceFileA            : " + reduceFileA);
            System.out.println("[DEBUG] \t- Input reduceFileB            : " + reduceFileB);
            System.out.println("[DEBUG] \t- Output reduceFileC           : " + reduceFileC);
            System.out.println("\n");
        }

        // Timer
        long startTime = System.currentTimeMillis();

        String newHeader = chrS.equals(ChromoInfo.MAX_NUMBER_OF_CHROMOSOMES_STR) ? GuidanceImpl.HEADER_MIXED_X : GuidanceImpl.HEADER_MIXED;
        String separator = " ";

        // Process A file
        MergeTwoChunksReader readerA = new MergeTwoChunksReader(reduceFileA, true);
        TreeMap<String, ArrayList<String>> fileATreeMap = null;
        try {
            fileATreeMap = readerA.read(separator, newHeader);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot process fileA", gte);
        }

        // Process B file
        MergeTwoChunksReader readerB = new MergeTwoChunksReader(reduceFileB, true);
        TreeMap<String, ArrayList<String>> fileBTreeMap = null;
        try {
            fileBTreeMap = readerB.read(separator, newHeader);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot process fileB", gte);
        }

        // A place to store the results of this merge
        TreeMap<String, ArrayList<String>> fileCTreeMap = new TreeMap<>();

        Iterator<Entry<String, ArrayList<String>>> iterA = fileATreeMap.entrySet().iterator();
        while (iterA.hasNext()) {
            // key=value separator this by Map.Entry to get key and value
            Entry<String, ArrayList<String>> m = iterA.next();
            // getKey is used to get key of Map
            String positionAndRsId = (String) m.getKey();

            // getValue is used to get value of key in Map
            ArrayList<String> fileTmp = m.getValue();
            // We look for the casesPosition key in the controlsTreeMap.
            // If found, we get the value, otherwise we get null
            fileCTreeMap.put(positionAndRsId, fileTmp);
        }
        Iterator<Entry<String, ArrayList<String>>> iterB = fileBTreeMap.entrySet().iterator();
        while (iterB.hasNext()) {
            // key=value separator this by Map.Entry to get key and value
            Entry<String, ArrayList<String>> m = iterB.next();
            // getKey is used to get key of Map
            // position=(Integer)m.getKey();
            String positionAndRsId = (String) m.getKey();

            // getValue is used to get value of key in Map
            ArrayList<String> fileTmp = m.getValue();
            // We look for the casesPosition key in the controlsTreeMap.
            // If found, we get the value, otherwise we get null
            fileCTreeMap.put(positionAndRsId, fileTmp);
        }

        // Finally we put the fileCTreeMap into the outputFile
        MergeTwoChunksWriter writer = new MergeTwoChunksWriter(reduceFileC);
        Hashtable<Integer, String> reversedTable = HashUtils.createHashWithHeaderReversed(newHeader, " ");
        try {
            writer.write(false, reversedTable, fileCTreeMap);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot write fileCTreeMap", gte);
        }

        // Then, we create the gz file and rename it
        try {
            GZFilesUtils.gzipFile(reduceFileC, reduceFileC + ".gz");
            File fc = new File(reduceFileC);
            File fGz = new File(reduceFileC + ".gz");
            GZFilesUtils.copyFile(fGz, fc);
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot zip reduce C file", ioe);
        }

        // Timer
        long stopTime = System.currentTimeMillis();
        long elapsedTime = (stopTime - startTime) / 1_000;
        if (DEBUG) {
            System.out.println("\n[DEBUG] mergeTwoChunks startTime: " + startTime);
            System.out.println("\n[DEBUG] mergeTwoChunks endTime: " + stopTime);
            System.out.println("\n[DEBUG] mergeTwoChunks elapsedTime: " + elapsedTime + " seconds");
            System.out.println("\n[DEBUG] Finished execution of mergeTwoChunks.");
        }
    }

    /**
     * Method to collect summary
     * 
     * @param chr
     * @param firstImputeFileInfo
     * @param snptestOutFile
     * @param reduceFile
     * @param mafThresholdS
     * @param infoThresholdS
     * @param hweCohortThresholdS
     * @param hweCasesThresholdS
     * @param hweControlsThresholdS
     * 
     * @throws GuidanceTaskException
     */
    public static void collectSummary(String chr, String firstImputeFileInfo, String snptestOutFile, String reduceFile,
            String mafThresholdS, String infoThresholdS, String hweCohortThresholdS, String hweCasesThresholdS,
            String hweControlsThresholdS) throws GuidanceTaskException {

        if (DEBUG) {
            System.out.println("\n");
            System.out.println("[DEBUG] Running collectSummary with parameters:");
            System.out.println("[DEBUG] \t- Input chromosome             : " + chr);
            System.out.println("[DEBUG] \t- Input casesImputeFileInfo    : " + firstImputeFileInfo);
            System.out.println("[DEBUG] \t- Input snptestOutFile         : " + snptestOutFile);
            System.out.println("[DEBUG] \t- Output reduceFile            : " + reduceFile);
            System.out.println("[DEBUG] \t- Input mafThresholdS          : " + mafThresholdS);
            System.out.println("[DEBUG] \t- Input infoThresholdS         : " + infoThresholdS);
            System.out.println("[DEBUG] \t- Input hweCohortThresholdS    : " + hweCohortThresholdS);
            System.out.println("[DEBUG] \t- Input hweCasesThresholdS     : " + hweCasesThresholdS);
            System.out.println("[DEBUG] \t- Input hweControlsThresholdS  : " + hweControlsThresholdS);
            System.out.println("\n");
        }

        // Timer
        long startTime = System.currentTimeMillis();

        // Process impute 2.3.2
        String separator = " ";
        ImputeFileReader readerImpute = new ImputeFileReader(firstImputeFileInfo, false);
        TreeMap<String, ArrayList<String>> imputeTreeMap = null;
        try {
            imputeTreeMap = readerImpute.read(separator);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot read first impute file", gte);
        }

        // We read each line of the snptestOutFile and put them into assocTreeMap array of Strings
        TreeMap<String, ArrayList<String>> assocTreeMap = new TreeMap<>();
        Hashtable<String, Integer> snptestHashTableIndex = null;
        Hashtable<Integer, String> snptestHashTableIndexReversed = null;
        int length_entry_assoc_list = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(snptestOutFile))))) {
            String line = null;
            String positionAndRsId = null;
            while ((line = br.readLine()) != null) {
                // We have to avoid reading all the comment lines that start with "#" character, and one more that is
                // the header and start with "alternate".
                // System.err.println("[DEBUGING1]: " + line);
                String[] splitted = line.split(" ");
                String firstHeader = splitted[0];
                char firstChar = line.charAt(0);

                // TODO: the next if is ugly
                if (firstHeader.equals("alternate_ids")) {
                    snptestHashTableIndex = HashUtils.createHashWithHeader(line, " ");
                    snptestHashTableIndexReversed = HashUtils.createHashWithHeaderReversed(line, " ");
                }

                if ((firstChar != '#') && (firstChar != 'a')) {
                    ArrayList<String> assocList = new ArrayList<>();
                    // delimiter I assume single space.
                    // System.err.println("[DEBUGING1]: " + line);
                    // String[] splitted = line.split(" ");

                    // REVIEW: Should we store position?
                    // assocList.add(splitted[3]);

                    // We store everything, from the line.
                    int index_field = 0;
                    for (index_field = 0; index_field < splitted.length; index_field++) {
                        assocList.add(splitted[index_field]);
                    }

                    // Now, store the array of string assocList in the assocTreeMap
                    positionAndRsId = splitted[snptestHashTableIndex.get("position")] + "_" + splitted[snptestHashTableIndex.get("rsid")];
                    if (!assocTreeMap.containsKey(positionAndRsId)) {
                        assocTreeMap.put(positionAndRsId, assocList);
                        length_entry_assoc_list = assocList.size();
                    } else {
                        assocTreeMap.remove(positionAndRsId);
                    }
                } // The line does not start with "#" niether "alternate"
            }
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot read SNP Test Out file", ioe);
        }

        // Now we process all the SNP information that are common
        TreeMap<String, ArrayList<String>> summaryTotal = new TreeMap<>();
        Iterator<Entry<String, ArrayList<String>>> iter = imputeTreeMap.entrySet().iterator();
        while (iter.hasNext()) {
            // key=value separator this by Map.Entry to get key and value
            Entry<String, ArrayList<String>> m = iter.next();
            // getKey is used to get key of Map
            String firstPositionAndRsId = (String) m.getKey();
            // getValue is used to get value of key in Map
            ArrayList<String> firstTmp = m.getValue();

            // The same for assocTreeMap. If found, we get the value, otherwise we get null
            ArrayList<String> assocTmp = assocTreeMap.get(firstPositionAndRsId);
            ArrayList<String> summaryTmp = null;
            try {
                mergeArrays(chr, firstTmp, assocTmp, length_entry_assoc_list, mafThresholdS, infoThresholdS, hweCohortThresholdS,
                        hweCasesThresholdS, hweControlsThresholdS);
            } catch (IOException ioe) {
                throw new GuidanceTaskException("[ERROR] Arrays cannot be merged", ioe);
            }

            summaryTotal.put(firstPositionAndRsId, summaryTmp);
            assocTreeMap.remove(firstPositionAndRsId);
        }

        // ---------------
        // Finally we put the summaryTotal into the outputFile
        CollectSummaryWriter writer = new CollectSummaryWriter(reduceFile);
        try {
            writer.write(false, snptestHashTableIndexReversed, summaryTotal);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot write summary total reduce File", gte);
        }

        // Then, we create the gz file and rename it
        try {
            GZFilesUtils.gzipFile(reduceFile, reduceFile + ".gz");
            File fb = new File(reduceFile);
            File fGz = new File(reduceFile + ".gz");
            GZFilesUtils.copyFile(fGz, fb);
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot zip reduce file", ioe);
        }

        // Timer
        long stopTime = System.currentTimeMillis();
        long elapsedTime = (stopTime - startTime) / 1_000;

        if (DEBUG) {
            System.out.println("\n[DEBUG] collectSummary startTime: " + startTime);
            System.out.println("\n[DEBUG] collectSummary endTime: " + stopTime);
            System.out.println("\n[DEBUG] collectSummary elapsedTime: " + elapsedTime + " seconds");
            System.out.println("\n[DEBUG] Finished execution of collectSummary.");
        }
    }

    /**
     * Method to initialize the Pheno matrix
     * 
     * @param topHitsFile
     * @param ttName
     * @param rpName
     * @param phenomeFile
     * 
     * @throws GuidanceTaskException
     */
    public static void initPhenoMatrix(String topHitsFile, String ttName, String rpName, String phenomeFile) throws GuidanceTaskException {
        if (DEBUG) {
            System.out.println("\n");
            System.out.println("[DEBUG] Running initPhenoMatrix with parameters:");
            System.out.println("[DEBUG] \t- Input topHitsFile       : " + topHitsFile);
            System.out.println("[DEBUG] \t- Input ttName            : " + ttName);
            System.out.println("[DEBUG] \t- Input rpName            : " + rpName);
            System.out.println("[DEBUG] \t- Output phenomeFile      : " + phenomeFile);
            System.out.println("\n");
        }

        // Timer
        long startTime = System.currentTimeMillis();

        // First, we load the whole topHitsFile into a TreeMap
        String separator = "\t";
        TopHitsReader readerTopHits = new TopHitsReader(topHitsFile, true);
        TreeMap<String, ArrayList<String>> phenomeTreeMap = null;
        try {
            phenomeTreeMap = readerTopHits.read(separator);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot read top hits file", gte);
        }

        // Finally, we print the phenomeThreeMap into the output file
        PhenoMatrixWriter writer = new PhenoMatrixWriter(phenomeFile);
        try {
            writer.write(false, null, phenomeTreeMap);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot write pheno matrix File", gte);
        }

        // Then, we create the gz file and rename it
        try {
            GZFilesUtils.gzipFile(phenomeFile, phenomeFile + ".gz");
            File fb = new File(phenomeFile);
            File fGz = new File(phenomeFile + ".gz");
            GZFilesUtils.copyFile(fGz, fb);
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot zip phenome File", ioe);
        }

        // Timer
        long stopTime = System.currentTimeMillis();
        long elapsedTime = (stopTime - startTime) / 1_000;

        if (DEBUG) {
            System.out.println("\n[DEBUG] initPhenoMatrix startTime: " + startTime);
            System.out.println("\n[DEBUG] initPhenoMatrix endTime: " + stopTime);
            System.out.println("\n[DEBUG] initPhenoMatrix elapsedTime: " + elapsedTime + " seconds");
            System.out.println("\n[DEBUG] Finished execution of initPhenoMatrix.");
        }
    }

    /**
     * Method to add to pheno matrix
     * 
     * @param phenomeAFile
     * @param topHitsFile
     * @param ttName
     * @param rpName
     * @param phenomeBFile
     * 
     * @throws GuidanceTaskException
     */
    public static void addToPhenoMatrix(String phenomeAFile, String topHitsFile, String ttName, String rpName, String phenomeBFile)
            throws GuidanceTaskException {

        if (DEBUG) {
            System.out.println("\n");
            System.out.println("[DEBUG] Running addToPhenoMatrix with parameters:");
            System.out.println("[DEBUG] \t- Input phenomeFileA      : " + phenomeAFile);
            System.out.println("[DEBUG] \t- Input filteredByAllFile : " + topHitsFile);
            System.out.println("[DEBUG] \t- Input ttName            : " + ttName);
            System.out.println("[DEBUG] \t- Input rpName            : " + rpName);
            System.out.println("[DEBUG] \t- Output phenomeFileB     : " + phenomeBFile);
            System.out.println("\n");
        }

        // Timer
        long startTime = System.currentTimeMillis();

        // First, we load the phenome A
        String separator = "\t";
        PartialPhenomeFileReader readerPhenomeA = new PartialPhenomeFileReader(phenomeAFile, true);
        TreeMap<String, ArrayList<String>> phenomeATreeMap = null;
        try {
            phenomeATreeMap = readerPhenomeA.read(separator);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot read phenome A file", gte);
        }

        // Next, we load the whole topHitsFile into a TreeMap
        separator = "\t";
        TopHitsReader readerTopHits = new TopHitsReader(topHitsFile, true);
        TreeMap<String, ArrayList<String>> topHitsTreeMap = null;
        try {
            topHitsTreeMap = readerTopHits.read(separator);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot read top hits file", gte);
        }

        // We add all non-repeated elements of topHits to A
        Iterator<Entry<String, ArrayList<String>>> topHitsIterator = topHitsTreeMap.entrySet().iterator();
        while (topHitsIterator.hasNext()) {
            Entry<String, ArrayList<String>> topHitsEntry = topHitsIterator.next();
            String chrAndPosition = topHitsEntry.getKey();

            // Add all entries of TopHits to phenomeA treeMap
            if (!phenomeATreeMap.containsKey(chrAndPosition)) {
                // Finally, we put this data into the phenomeBTreeMap
                phenomeATreeMap.put(chrAndPosition, topHitsEntry.getValue());
            }
        }

        // Finally, we print the phenomeThreeMap into the output file
        PhenoMatrixWriter writer = new PhenoMatrixWriter(phenomeBFile);
        try {
            writer.write(false, null, phenomeATreeMap);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot write pheno matrix File", gte);
        }

        // Then, we create the gz file and rename it
        try {
            GZFilesUtils.gzipFile(phenomeBFile, phenomeBFile + ".gz");
            File fb = new File(phenomeBFile);
            File fGz = new File(phenomeBFile + ".gz");
            GZFilesUtils.copyFile(fGz, fb);
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot zip phenome B File", ioe);
        }

        // Timer
        long stopTime = System.currentTimeMillis();
        long elapsedTime = (stopTime - startTime) / 1_000;
        if (DEBUG) {
            System.out.println("\n[DEBUG] addToPhenoMatrix startTime: " + startTime);
            System.out.println("\n[DEBUG] addToPhenoMatrix endTime: " + stopTime);
            System.out.println("\n[DEBUG] addToPhenoMatrix elapsedTime: " + elapsedTime + " seconds");
            System.out.println("\n[DEBUG] Finished execution of addToPhenoMatrix.");
        }
    }

    /**
     * Method to fill out the Pheno Matrix
     * 
     * @param phenomeAFile
     * @param filteredByAllFile
     * @param filteredByAllXFile
     * @param endChrS
     * @param ttName
     * @param rpName
     * @param phenomeBFile
     * 
     * @throws GuidanceTaskException
     */
    public static void filloutPhenoMatrix(String phenomeAFile, String filteredByAllFile, String filteredByAllXFile, String endChrS,
            String ttName, String rpName, String phenomeBFile) throws GuidanceTaskException {

        if (DEBUG) {
            System.out.println("\n");
            System.out.println("[DEBUG] Running filloutPhenoMatrix with parameters:");
            System.out.println("[DEBUG] \t- Input phenomeFileA       : " + phenomeAFile);
            System.out.println("[DEBUG] \t- Input filteredByAllFile  : " + filteredByAllFile);
            System.out.println("[DEBUG] \t- Input filteredByAllXFile : " + filteredByAllXFile);
            System.out.println("[DEBUG] \t- Input endChrS            : " + endChrS);
            System.out.println("[DEBUG] \t- Input ttName             : " + ttName);
            System.out.println("[DEBUG] \t- Input rpName             : " + rpName);
            System.out.println("[DEBUG] \t- Output phenomeFileB      : " + phenomeBFile);
            System.out.println("\n");
        }
        long startTime = System.currentTimeMillis();

        // TODO HERE
        // First we read the phenomeAFile and load the information into the phenomeATreeMap
        String separator = "\t";
        Hashtable<Integer, String> phenomeAHashTableIndexReversed = new Hashtable<>();
        FullPhenomeFileReader readerPhenomeA = new FullPhenomeFileReader(phenomeAFile, true);
        TreeMap<String, ArrayList<String>> phenomeATreeMap = null;
        try {
            phenomeATreeMap = readerPhenomeA.read(separator, false, ttName, rpName, phenomeAHashTableIndexReversed);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot read phenome A file", gte);
        }

        // Then, we need to extract the information of each snp from the filteredByAllFile
        // Now we load the whole filteredByAllFile into a TreeMap
        separator = "\t";
        FilteredByAllReader readerFiltered = new FilteredByAllReader(filteredByAllFile, true);
        TreeMap<String, ArrayList<String>> filteredTreeMap = null;
        try {
            filteredTreeMap = readerFiltered.read(separator);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot read filtered by all file", gte);
        }

        // Here we have to do some similar with filteredByAllXFile (the results for chr23)
        TreeMap<String, ArrayList<String>> filteredXTreeMap = null;
        if (endChrS.equals(ChromoInfo.MAX_NUMBER_OF_CHROMOSOMES_STR)) {
            separator = "\t";
            FilteredByAllXReader readerFilteredX = new FilteredByAllXReader(filteredByAllXFile, true);
            try {
                filteredXTreeMap = readerFilteredX.read(separator);
            } catch (GuidanceTaskException gte) {
                throw new GuidanceTaskException("[ERROR] Cannot read filtered by all X file", gte);
            }
        }

        // Now, we print the information of each snp from filteredByAllFile into the phenomeATreeMap
        Iterator<Entry<String, ArrayList<String>>> iter = phenomeATreeMap.entrySet().iterator();
        while (iter.hasNext()) {
            // key=value separator this by Map.Entry to get key and value
            Entry<String, ArrayList<String>> m = iter.next();

            ArrayList<String> currentList = m.getValue();
            ArrayList<String> reducedList = null;

            // getKey is used to get key of Map
            String chrAndPosition = m.getKey();

            String[] divideKey = chrAndPosition.split("_");
            String Chr = divideKey[0];

            if (!Chr.equals(ChromoInfo.MAX_NUMBER_OF_CHROMOSOMES_STR)) {
                if (filteredTreeMap.containsKey(chrAndPosition)) {
                    reducedList = filteredTreeMap.get(chrAndPosition);

                    for (int i = 0; i < reducedList.size(); i++) {
                        currentList.add(reducedList.get(i));
                    }
                } else {
                    for (int i = 0; i < 11; i++) {
                        currentList.add("NA");
                    }
                }
            } else {
                if (filteredXTreeMap.containsKey(chrAndPosition)) {
                    reducedList = filteredXTreeMap.get(chrAndPosition);
                    for (int i = 0; i < reducedList.size(); i++) {
                        currentList.add(reducedList.get(i));
                    }
                } else {
                    for (int i = 0; i < 11; i++) {
                        currentList.add("NA");
                    }
                }
            }

            phenomeATreeMap.put(chrAndPosition, currentList);
        }

        // Finally, we print the phenomeAThreeMap into the output file
        PhenoMatrixWriter writer = new PhenoMatrixWriter(phenomeBFile);
        try {
            writer.write(false, null, phenomeATreeMap);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot write pheno matrix File", gte);
        }

        // Then, we create the gz file and rename it
        try {
            GZFilesUtils.gzipFile(phenomeBFile, phenomeBFile + ".gz");
            File fb = new File(phenomeBFile);
            File fGz = new File(phenomeBFile + ".gz");
            GZFilesUtils.copyFile(fGz, fb);
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot zip phenome B File", ioe);
        }

        // Timer
        long stopTime = System.currentTimeMillis();
        long elapsedTime = (stopTime - startTime) / 1_000;

        if (DEBUG) {
            System.out.println("\n[DEBUG] filloutPhenoMatrix startTime: " + startTime);
            System.out.println("\n[DEBUG] filloutPhenoMatrix endTime: " + stopTime);
            System.out.println("\n[DEBUG] filloutPhenoMatrix elapsedTime: " + elapsedTime + " seconds");
            System.out.println("\n[DEBUG] Finished execution of filloutPhenoMatrix.");
        }

    }

    /**
     * Method to finalize the Pheno Matrix
     * 
     * @param phenomeAFile
     * @param phenomeBFile
     * @param ttName
     * @param rpName
     * @param phenomeCFile
     * 
     * @throws GuidanceTaskException
     */
    public static void finalizePhenoMatrix(String phenomeAFile, String phenomeBFile, String ttName, String rpName, String phenomeCFile)
            throws GuidanceTaskException {

        if (DEBUG) {
            System.out.println("\n");
            System.out.println("[DEBUG] Running finalizePhenoMatrix with parameters:");
            System.out.println("[DEBUG] \t- Input phenomeAFile  : " + phenomeAFile);
            System.out.println("[DEBUG] \t- Input phenomeBFile  : " + phenomeBFile);
            System.out.println("[DEBUG] \t- Input ttName        : " + ttName);
            System.out.println("[DEBUG] \t- Input rpName        : " + rpName);
            System.out.println("[DEBUG] \t- Output phenomeCFile : " + phenomeCFile);
            System.out.println("\n");
        }

        // Timer
        long startTime = System.currentTimeMillis();

        // First we read the phenomeAFile and load the information into the phenomeATreeMap
        String separator = "\t";
        Hashtable<Integer, String> phenomeAHashTableIndexReversed = new Hashtable<>();
        FullPhenomeFileReader readerPhenomeA = new FullPhenomeFileReader(phenomeAFile, true);
        TreeMap<String, ArrayList<String>> phenomeATreeMap = null;
        try {
            phenomeATreeMap = readerPhenomeA.read(separator, true, ttName, rpName, phenomeAHashTableIndexReversed);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot read phenome A file", gte);
        }

        // Second we read the phenomeBFile and load the information into the phenomeBTreeMap
        separator = "\t";
        Hashtable<Integer, String> phenomeBHashTableIndexReversed = new Hashtable<>();
        FullPhenomeFileReader readerPhenomeB = new FullPhenomeFileReader(phenomeBFile, true);
        TreeMap<String, ArrayList<String>> phenomeBTreeMap = null;
        try {
            phenomeBTreeMap = readerPhenomeB.read(separator, true, ttName, rpName, phenomeBHashTableIndexReversed);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot read phenome B file", gte);
        }

        // Third we merge by adding the B content to the line of the A content
        Iterator<Entry<String, ArrayList<String>>> iterB = phenomeBTreeMap.entrySet().iterator();
        while (iterB.hasNext()) {
            Entry<String, ArrayList<String>> entryB = iterB.next();
            String chrAndPosition = entryB.getKey();
            if (phenomeATreeMap.containsKey(chrAndPosition)) {
                ArrayList<String> mergedList = phenomeATreeMap.get(chrAndPosition);
                mergedList.addAll(entryB.getValue());
                phenomeATreeMap.put(chrAndPosition, mergedList);
            }
        }

        // Finally, we print the phenomeAThreeMap into the output file
        String finalHeader = "";
        for (int i = 0; i < phenomeAHashTableIndexReversed.size(); ++i) {
            finalHeader = finalHeader + "\t" + phenomeAHashTableIndexReversed.get(i);
        }
        for (int i = 2; i < phenomeBHashTableIndexReversed.size(); i++) {
            finalHeader = finalHeader + "\t" + phenomeBHashTableIndexReversed.get(i);
        }

        PhenoMatrixWriter writer = new PhenoMatrixWriter(phenomeCFile);
        try {
            writer.write(false, null, phenomeATreeMap);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot write pheno matrix File", gte);
        }

        // Then, we create the gz file and rename it
        try {
            GZFilesUtils.gzipFile(phenomeCFile, phenomeCFile + ".gz");
            File fb = new File(phenomeCFile);
            File fGz = new File(phenomeCFile + ".gz");
            GZFilesUtils.copyFile(fGz, fb);
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot zip phenome C File", ioe);
        }

        // Timer
        long stopTime = System.currentTimeMillis();
        long elapsedTime = (stopTime - startTime) / 1_000;

        if (DEBUG) {
            System.out.println("\n[DEBUG] finalizePhenoMatrix startTime: " + startTime);
            System.out.println("\n[DEBUG] finalizePhenoMatrix endTime: " + stopTime);
            System.out.println("\n[DEBUG] finalizePhenoMatrix elapsedTime: " + elapsedTime + " seconds");
            System.out.println("\n[DEBUG] Finished execution of finalizePhenoMatrix.");
        }
    }

    /**
     * Pre actions to the GenerateQQManhattanPlots binary task
     * 
     * @param condensedFile
     * @param condensedFileUnzip
     * 
     * @throws GuidanceTaskException
     */
    public static void preGenerateQQManhattanPlots(String condensedFile, String condensedFileUnzip) throws GuidanceTaskException {
        try {
            GZFilesUtils.gunzipFile(condensedFile, condensedFileUnzip);
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot uncompress pre generateQQManhattanPlots files", ioe);
        }
    }

    /**
     * Post actions to generate the SNP Test GZ file
     * 
     * @param snpTestOutFile
     * @param snpTestOutFileGz
     * 
     * @throws GuidanceTaskException
     */
    public static void postSnptest(String snpTestOutFile, String snpTestOutFileGz) throws GuidanceTaskException {
        try {
            GZFilesUtils.gzipFile(snpTestOutFile, snpTestOutFileGz);
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot compress the SNP Test output files", ioe);
        }
    }

    /**
     * Post actions to generate the QCTool GZ File
     * 
     * @param filteredFile
     * @param filteredFileGz
     * 
     * @throws GuidanceTaskException
     */
    public static void postQCTool(String filteredFile, String filteredFileGz) throws GuidanceTaskException {
        try {
            GZFilesUtils.gzipFile(filteredFile, filteredFileGz);
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot compress the SNP Test output files", ioe);
        }

        // Then we rename it
        // File fFilteredGz = new File(filteredFileGz);
        // File fFiltered = new File(filteredFile);
        // copyFile(fFilteredGz, fFiltered);
    }

    /**
     * Method to return the Allele
     * 
     * @param allele1
     * @param allele2
     * @param typeAllele
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws Exception
     */
    private static String getAllele(String allele1, String allele2, String typeAllele) throws GuidanceTaskException {
        // Lets compute the reverse of allele1
        // String a1Reverse = new StringBuffer().reverse(allele1).toString();

        // Lets compute the reverse of allele2
        // String a2Reverse = new StringBuffer().reverse(allele2).toString();

        // Lets compute the complement of allele1
        String a1Complement = "";
        for (int i = 0; i < allele1.length(); i++) {
            char aChar = getComplement(allele1.charAt(i));
            a1Complement = a1Complement + aChar;
        }

        // Lets compute the complement of allele2
        String a2Complement = "";
        for (int i = 0; i < allele2.length(); i++) {
            char aChar = getComplement(allele2.charAt(i));
            a2Complement = a2Complement + aChar;
        }

        // Lets compute the reverse of a1Complement
        // String a1ComplementAndReverse = new StringBuffer().reverse(a1Complement).toString();
        // Lets compute the reverse of a2Complement
        // String a2ComplementAndReverse = new StringBuffer().reverse(a2Complement).toString();

        if (typeAllele.equals("reverse")) {
            return allele2 + "_" + allele1;
        } else if (typeAllele.equals("complement")) {
            return a1Complement + "_" + a2Complement;
        } else if (typeAllele.equals("complementAndReverse")) {
            return a2Complement + "_" + a1Complement;
        } else {
            throw new GuidanceTaskException("[Error] The option (" + typeAllele + ") for creating alternative alleles is now correct!!");
        }
    }

    /**
     * Returns the complement of an allele
     * 
     * @param allele
     * @return
     */
    private static char getComplement(char allele) {
        char res;
        switch (allele) {
            case 'A':
                res = 'T';
                break;
            case 'T':
                res = 'A';
                break;
            case 'C':
                res = 'G';
                break;
            case 'G':
                res = 'C';
                break;
            default:
                res = 'X';
        }

        return res;
    }

    /**
     * Method to merge Arrays
     * 
     * @param chr
     * @param caseArray
     * @param assocArray
     * @param length_entry_assoc_list
     * @param mafThresholdS
     * @param infoThresholdS
     * @param hweCohortThresholdS
     * @param hweCasesThresholdS
     * @param hweControlsThresholdS
     * @return
     * 
     * @throws IOException
     */
    private static ArrayList<String> mergeArrays(String chr, ArrayList<String> caseArray, ArrayList<String> assocArray,
            int length_entry_assoc_list, String mafThresholdS, String infoThresholdS, String hweCohortThresholdS, String hweCasesThresholdS,
            String hweControlsThresholdS) throws IOException {

        // Double mafThreshold = Double.parseDouble(mafThresholdS);
        // Double infoThreshold = Double.parseDouble(infoThresholdS);
        // Double hweCohortThreshold = Double.parseDouble(hweCohortThresholdS);
        // Double hweCasesThreshold = Double.parseDouble(hweCasesThresholdS);
        // Double hweControlsThreshold = Double.parseDouble(hweCohortThresholdS);
        int real_length_assoc = 0;
        if (chr.equals(ChromoInfo.MAX_NUMBER_OF_CHROMOSOMES_STR)) {
            real_length_assoc = 67;
        } else {
            // real_length_assoc = 69;
            real_length_assoc = 67;
        }

        ArrayList<String> summaryTmp = new ArrayList<>();
        // We just need to put the information of the different arraysList into the returned arrayList
        // First the position (as string):

        // We put the chromosome number
        summaryTmp.add(chr);

        if (caseArray != null) {
            for (int i = 0; i < caseArray.size(); i++) {
                summaryTmp.add(caseArray.get(i));
            }
        } else {
            System.out.println("\n[DEBUG] Extranisimo, este caso no deberia darse.");
        }

        if (assocArray != null) {
            int assoc_length = assocArray.size();
            // We do not store the first 4 field because they are not necessary or are repeated:
            // These four fields are:
            // alternative_ids, rsid, chromosome, position
            for (int i = 4; i < assoc_length; i++) {
                summaryTmp.add(assocArray.get(i));
            }
        } else {
            // 67 is the number of field in a mixed assocArray
            for (int i = 4; i < real_length_assoc; i++) {
                summaryTmp.add("NA");
            }
        }

        return summaryTmp;
    }

}
