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
import guidance.executors.CombineCondensedFilesExecutor;
import guidance.executors.CreateListOfExcludedSnpsExecutor;
import guidance.executors.CreateRsIdListExecutor;
import guidance.executors.FilterByInfoExecutor;
import guidance.executors.GenerateTopHitsAllExecutor;
import guidance.executors.JointCondensedFilesExecutor;
import guidance.executors.JointFilteredByAllFilesExecutor;
import guidance.executors.PostFilterHaplotypesExecutor;
import guidance.utils.ChromoInfo;
import guidance.utils.GZFilesUtils;
import guidance.utils.HashUtils;
import guidance.writers.CollectSummaryWriter;
import guidance.writers.PhenoMatrixWriter;
import guidance.writers.MergeTwoChunksWriter;

import java.io.File;
import java.io.Reader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Hashtable;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.Scanner;


public class GuidanceImpl {

    // Debug
    private static final boolean DEBUG = true;

    // Headers
    private static final String HEADER_MIXED = "chr	position	rs_id_all	info_all	certainty_all	alleleA	alleleB	index	average_maximum_posterior_call	info	cohort_1_AA	cohort_1_AB	cohort_1_BB	cohort_1_NULL	all_AA	all_AB	all_BB	all_NULL	all_total	cases_AA	cases_AB	cases_BB	cases_NULL	cases_total	controls_AA	controls_AB	controls_BB	controls_NULL	controls_total	all_maf	cases_maf	controls_maf	missing_data_proportion	cohort_1_hwe	cases_hwe	controls_hwe	het_OR	het_OR_lower	het_OR_upper	hom_OR	hom_OR_lower	hom_OR_upper	all_OR	all_OR_lower	all_OR_upper	frequentist_add_pvalue	frequentist_add_info	frequentist_add_beta_1	frequentist_add_se_1	frequentist_dom_pvalue	frequentist_dom_info	frequentist_dom_beta_1	frequentist_dom_se_1	frequentist_rec_pvalue	frequentist_rec_info	frequentist_rec_beta_1	frequentist_rec_se_1	frequentist_gen_pvalue	frequentist_gen_info	frequentist_gen_beta_1	frequentist_gen_se_1	frequentist_gen_beta_2	frequentist_gen_se_2	frequentist_het_pvalue	frequentist_het_info	frequentist_het_beta_1	frequentist_het_se_1	comment";
    private static final String HEADER_MIXED_X = "chr	position	rs_id_all	info_all	certainty_all	alleleA	alleleB	all_A	all_B	all_AA	all_AB	all_BB	all_NULL	all_total	all_maf	all_info	all_impute_info	cases_A	cases_B	cases_AA	cases_AB	cases_BB	cases_NULL	cases_total	cases_maf	cases_info	cases_impute_info	controls_A	controls_B	controls_AA	controls_AB	controls_BB	controls_NULL	controls_total	controls_maf	controls_info	controls_impute_info	sex=1_A	sex=1_B	sex=1_AA	sex=1_AB	sex=1_BB	sex=1_NULL	sex=1_total	sex=1_maf	sex=1_info	sex=1_impute_info	sex=2_A	sex=2_B	sex=2_AA	sex=2_AB	sex=2_BB	sex=2_NULL	sex=2_total	sex=2_maf	sex=2_info	sex=2_impute_info	frequentist_add_null_ll	frequentist_add_alternative_ll	frequentist_add_beta_1:genotype/sex=1	frequentist_add_beta_2:genotype/sex=2	frequentist_add_se_1:genotype/sex=1	frequentist_add_se_2:genotype/sex=2	frequentist_add_degrees_of_freedom	frequentist_add_pvalue	comment";

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
        CreateRsIdListExecutor executor = new CreateRsIdListExecutor(genOrBimFile, pairsFile, isFileGz, false);
        try {
            executor.execute(exclCgatFlag, inputFormat);
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
        CreateListOfExcludedSnpsExecutor executor = new CreateListOfExcludedSnpsExecutor(shapeitHapsFile, excludedSnpsFile, true, false);
        try {
            executor.execute(exclCgatFlag, exclSVFlag, rsIdIndex, posIndex, a1Index, a2Index, separator);
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
        PostFilterHaplotypesExecutor executor = new PostFilterHaplotypesExecutor(filteredHapsFile, listOfSnpsFile, true, false);
        try {
            executor.execute();
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
        FilterByInfoExecutor executor = new FilterByInfoExecutor(imputeFileInfo, filteredFile, false, false);
        try {
            executor.execute(infoIndex, rsIdIndex, thresholdDouble);
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

        // Try to create the output file
        File outFilteredFile = new File(outputFile);
        try {
            boolean success = outFilteredFile.createNewFile();
            if (!success) {
                throw new GuidanceTaskException("[ERROR] Cannot create output file. CreateNewFile returned non-zero exit value");
            }
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot create output file", ioe);
        }
        // Print information about de existence of the file
        System.out.println("\n[DEBUG] \t- Output file " + outputFile + " succesfuly created");

        // Try to create the output condensed file
        File outCondensedFile = new File(outputCondensedFile);
        try {
            boolean success = outCondensedFile.createNewFile();
            if (!success) {
                throw new GuidanceTaskException("[ERROR] Cannot create output condensed file. CreateNewFile returned non-zero exit value");
            }
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot create output condensed file", ioe);
        }
        // Print information about de existence of the file
        System.out.println("\n[DEBUG] \t- Output file " + outputCondensedFile + " succesfuly created");

        // Process input
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(inputFile))));
                BufferedWriter writerFiltered = new BufferedWriter(new FileWriter(outFilteredFile));
                BufferedWriter writerCondensed = new BufferedWriter(new FileWriter(outCondensedFile))) {

            // I read the header
            String line = br.readLine();
            // Put the header in the output file.
            writerFiltered.write(line);
            writerFiltered.newLine();

            Hashtable<String, Integer> inputFileHashTableIndex = HashUtils.createHashWithHeader(line, "\t");
            Hashtable<Integer, String> inputFileHashTableIndexReversed = HashUtils.createHashWithHeaderReversed(line, "\t");

            String headerCondensed = "CHR\tBP\tP";
            writerCondensed.write(headerCondensed);
            writerCondensed.newLine();

            while ((line = br.readLine()) != null) {
                String[] splittedLine = line.split("\t");// delimiter I assume single space.
                String chromo = splittedLine[inputFileHashTableIndex.get("chr")];
                String infoS = splittedLine[inputFileHashTableIndex.get("info_all")];

                // We start with these values for hwe values just to allows the X chromosome to pass the if statement of
                // the next lines
                // Just remember that hwe filtering when chromo X is being processed does not make sense.
                String hwe_cohortS = "1.0";
                String hwe_casesS = "1.0";
                String hwe_controlsS = "1.0";

                if (!chromo.equals(ChromoInfo.MAX_NUMBER_OF_CHROMOSOMES_STR)) {
                    hwe_cohortS = splittedLine[inputFileHashTableIndex.get("cohort_1_hwe")];
                    hwe_casesS = splittedLine[inputFileHashTableIndex.get("cases_hwe")];
                    hwe_controlsS = splittedLine[inputFileHashTableIndex.get("controls_hwe")];
                }

                String cases_mafS = splittedLine[inputFileHashTableIndex.get("cases_maf")];
                String controls_mafS = splittedLine[inputFileHashTableIndex.get("controls_maf")];

                String position = splittedLine[inputFileHashTableIndex.get("position")];
                // String beta = splittedLine[inputFileHashTableIndex.get("frequentist_add_beta_1")];
                // String se = splittedLine[inputFileHashTableIndex.get("frequentist_add_se_1")];
                String pva = splittedLine[inputFileHashTableIndex.get("frequentist_add_pvalue")];

                String chrbpb = chromo + "\t" + position + "\t" + pva;

                if (!cases_mafS.equals("NA") && !controls_mafS.equals("NA") && !infoS.equals("NA") && !hwe_cohortS.equals("NA")
                        && !hwe_casesS.equals("NA") && !hwe_controlsS.equals("NA") && !pva.equals("NA")) {
                    Double cases_maf = Double.parseDouble(cases_mafS);
                    Double controls_maf = Double.parseDouble(controls_mafS);
                    Double info = Double.parseDouble(infoS);
                    Double hweCohort = 1.0;
                    Double hweCases = 1.0;
                    Double hweControls = 1.0;

                    if (!chromo.equals(ChromoInfo.MAX_NUMBER_OF_CHROMOSOMES_STR)) {
                        hweCohort = Double.parseDouble(hwe_cohortS);
                        hweCases = Double.parseDouble(hwe_casesS);
                        hweControls = Double.parseDouble(hwe_controlsS);
                    }

                    // Verificar las condiciones
                    if (cases_maf >= mafThreshold && controls_maf >= mafThreshold && info >= infoThreshold
                            && hweCohort >= hweCohortThreshold && hweCases >= hweCasesThreshold && hweControls >= hweControlsThreshold) {

                        writerFiltered.write(line);
                        writerFiltered.newLine();

                        writerCondensed.write(chrbpb);
                        writerCondensed.newLine();
                    }
                }
            }

            writerFiltered.flush();
            writerCondensed.flush();
        } catch (IOException ioe) {
            throw new GuidanceTaskException(ioe);
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
        JointFilteredByAllFilesExecutor executor = new JointFilteredByAllFilesExecutor(filteredByAllA, filteredByAllC, true, false);
        try {
            executor.execute(rpanelName);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot process A file", gte);
        }

        // Do the same with the filteredByAllB file if this file is different to the filteredByAllA file
        // WARN: The only way that filteredByAllB = filteredByAllA is when there is only one chromosome to process
        // In that case, the main program sends the same file as filteredByAllA and filteredByAllB
        // Process B input
        if (!filteredByAllA.equals(filteredByAllB)) {
            executor = new JointFilteredByAllFilesExecutor(filteredByAllB, filteredByAllC, true, true);
            try {
                executor.execute(rpanelName);
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
        JointCondensedFilesExecutor executor = new JointCondensedFilesExecutor(inputAFile, outputFile, true, false);
        try {
            executor.execute();
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot process A file", gte);
        }

        // Do the same with the filteredByAllB file if this file is different to the filteredByAllA file
        // WARN: The only way that filteredByAllB = filteredByAllA is when there is only one chromosome to process
        // In that case, the main program sends the same file as filteredByAllA and filteredByAllB
        // Process B input
        if (!inputAFile.equals(inputBFile)) {
            executor = new JointCondensedFilesExecutor(inputBFile, outputFile, true, true);
            try {
                executor.execute();
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

        /*
         * int posIdx = 0; int a1Idx = 4; int a2Idx = 5; int chrIdx = 35; int infoIdx = 2;
         */
        int posIdx = 0;
        int a1Idx = 0;
        int a2Idx = 0;
        int chrIdx = 0;
        int infoIdx = 0;

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

        // We have to create the outputFile for this combination:
        // We verify that a file with the same name does not exist.
        File outputFile = new File(resultsPanelC);
        outputFile.createNewFile();

        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

        // We read each line of the resultsPanelA and put them into the String
        // IMPORTANT: In that case we sort by position and not by position_rsID. So, maybe we
        // are going to lose some SNPs...
        for (int chromo = chrStart; chromo <= chrEnd; chromo++) {
            Hashtable<String, Integer> resultsHashTableIndex = new Hashtable<String, Integer>();
            String chromoS = Integer.toString(chromo);

            // Create the first treeMap for the chromo
            TreeMap<String, String> fileTreeMapA = new TreeMap<String, String>();

            String header = null;
            String positionA1A2Chr = null;
            try (Scanner sc1 = new Scanner(new File(resultsPanelA + ".temp"))) {
                // Get the header
                header = sc1.nextLine();
                resultsHashTableIndex = HashUtils.createHashWithHeader(header, "\t");
                chrIdx = resultsHashTableIndex.get("chr");
                posIdx = resultsHashTableIndex.get("position");
                a1Idx = resultsHashTableIndex.get("alleleA");
                a2Idx = resultsHashTableIndex.get("alleleB");
                infoIdx = resultsHashTableIndex.get("info_all");

                while (sc1.hasNextLine()) {
                    String line = sc1.nextLine();
                    String[] splitted = line.split("\t");

                    if (splitted[chrIdx].equals(chromoS)) {
                        positionA1A2Chr = splitted[posIdx] + "_" + splitted[a1Idx] + "_" + splitted[a2Idx] + "_" + splitted[chrIdx];
                        // Now, we put this String into the treemap with the key positionA1A1Chr
                        fileTreeMapA.put(positionA1A2Chr, line);
                        // contador++;
                    }
                }
                // note that Scanner suppresses exceptions
                if (sc1.ioException() != null) {
                    throw sc1.ioException();
                }
                sc1.close();
            }
            // System.out.println("i\n[DEBUG] We have read the chromo " + chromoS + " from first File. contador = " +
            // contador);

            // Create the second treeMap for the chromo
            TreeMap<String, String> fileTreeMapB = new TreeMap<String, String>();
            // contador=0;
            try (Scanner sc = new Scanner(new File(resultsPanelB + ".temp"))) {
                // Get the header
                header = sc.nextLine();
                chrIdx = resultsHashTableIndex.get("chr");
                posIdx = resultsHashTableIndex.get("position");
                a1Idx = resultsHashTableIndex.get("alleleA");
                a2Idx = resultsHashTableIndex.get("alleleB");
                infoIdx = resultsHashTableIndex.get("info_all");

                while (sc.hasNextLine()) {
                    String line = sc.nextLine();
                    String[] splitted = line.split("\t");

                    if (splitted[chrIdx].equals(chromoS)) {
                        positionA1A2Chr = splitted[posIdx] + "_" + splitted[a1Idx] + "_" + splitted[a2Idx] + "_" + splitted[chrIdx];
                        // Now, we put this String into the treemap with the key positionA1A1Chr
                        fileTreeMapB.put(positionA1A2Chr, line);
                        // contador++;
                    }
                }
                // note that Scanner suppresses exceptions
                if (sc.ioException() != null) {
                    throw sc.ioException();
                }
                sc.close();
            }
            // System.out.println("\n[DEBUG] We have read the chromo " + chromoS + " from second File. contador = " +
            // contador);

            // A place to store the results of this combining
            String lineA = null;
            String lineB = null;

            String[] splittedA = null;
            String[] splittedB = null;

            Double infoA;
            Double infoB;

            String posAllelesEqual = null;
            String posAllelesReverse = null;
            String posAllelesComplement = null;
            String posAllelesComplementAndReverse = null;

            TreeMap<String, String> fileTreeMapC = new TreeMap<String, String>();
            // contador=0;

            // We first iterate the fileTreeMapA
            Set<Entry<String, String>> mySet = fileTreeMapA.entrySet();
            // Move next key and value of Map by iterator
            Iterator<Entry<String, String>> iter = mySet.iterator();
            while (iter.hasNext()) {
                // key=value separator this by Map.Entry to get key and value
                Entry<String, String> m = iter.next();
                positionA1A2Chr = (String) m.getKey();
                lineA = (String) m.getValue();
                splittedA = lineA.split("\t");
                infoA = Double.parseDouble(splittedA[infoIdx]);

                posAllelesEqual = positionA1A2Chr;
                posAllelesReverse = splittedA[posIdx] + "_" + getAllele(splittedA[a1Idx], splittedA[a2Idx], "reverse") + "_"
                        + splittedA[chrIdx];
                posAllelesComplement = splittedA[posIdx] + "_" + getAllele(splittedA[a1Idx], splittedA[a2Idx], "complement") + "_"
                        + splittedA[chrIdx];
                posAllelesComplementAndReverse = splittedA[posIdx] + "_"
                        + getAllele(splittedA[a1Idx], splittedA[a2Idx], "complementAndReverse") + "_" + splittedA[chrIdx];

                // System.out.println("[combinePanelsComplex] " + positionA1A2Chr + " " + posAllelesEqual + " " +
                // posAllelesReverse + " " + posAllelesComplement + " " + posAllelesComplementAndReverse);

                // The same: position, a1 and a2?
                if (fileTreeMapB.containsKey(positionA1A2Chr)) {
                    // If the fileTreeMapB contains this positionA1A2Chr combination, then we have to choose
                    // the ones that has a better info (that is the ones with greater info).
                    lineB = fileTreeMapB.get(positionA1A2Chr);
                    splittedB = lineB.split("\t");
                    infoB = Double.parseDouble(splittedB[infoIdx]);
                    // Then we have to choose between A o B.
                    if (infoA >= infoB) {
                        fileTreeMapC.put(positionA1A2Chr, lineA);
                    } else {
                        fileTreeMapC.put(positionA1A2Chr, lineB);
                    }
                    // System.out.println("WOW alelos iguales: " + positionA1A2Chr);

                    // Now we remove this value from the fileTreeMapB
                    fileTreeMapB.remove(positionA1A2Chr);
                } else if (fileTreeMapB.containsKey(posAllelesReverse)) {
                    // If the fileTreeMapB contains this posAllelesReverse, then we have to choose
                    // the ones that has a better info (that is the ones with greater info).
                    lineB = fileTreeMapB.get(posAllelesReverse);
                    splittedB = lineB.split("\t");
                    infoB = Double.parseDouble(splittedB[infoIdx]);
                    // Then we have to choose between A and B.
                    if (infoA >= infoB) {
                        fileTreeMapC.put(positionA1A2Chr, lineA);
                    } else {
                        fileTreeMapC.put(posAllelesReverse, lineB);
                    }
                    // Now we remove this value from the fileTreeMapB
                    fileTreeMapB.remove(posAllelesReverse);
                    // System.out.println("WOW alelos reversos: " + positionA1A2Chr + " " + posAllelesReverse);
                } else if (fileTreeMapB.containsKey(posAllelesComplement)) {
                    // If the fileTreeMapB contains this posAllelesComplement, then we have to choose
                    // the ones that has a better info (that is the ones with greater info).
                    lineB = fileTreeMapB.get(posAllelesComplement);
                    splittedB = lineB.split("\t");
                    infoB = Double.parseDouble(splittedB[infoIdx]);
                    // Then we have to choose between A o B.
                    if (infoA >= infoB) {
                        fileTreeMapC.put(positionA1A2Chr, lineA);
                    } else {
                        fileTreeMapC.put(posAllelesComplement, lineB);
                    }
                    // Now we remove this value from the fileTreeMapB
                    fileTreeMapB.remove(posAllelesComplement);
                    // System.out.println("WOW alelos complementarios: " + positionA1A2Chr + " " +
                    // posAllelesComplement);
                } else if (fileTreeMapB.containsKey(posAllelesComplementAndReverse)) {
                    // If the fileTreeMapB contains this posAllelesComplement, then we have to choose
                    // the ones that has a better info (that is the ones with greater info).
                    lineB = fileTreeMapB.get(posAllelesComplementAndReverse);
                    splittedB = lineB.split("\t");
                    infoB = Double.parseDouble(splittedB[infoIdx]);
                    // Then we have to choose between A o B.
                    if (infoA >= infoB) {
                        fileTreeMapC.put(positionA1A2Chr, lineA);
                    } else {
                        fileTreeMapC.put(posAllelesComplementAndReverse, lineB);
                    }
                    // Now we remove this value from the fileTreeMapB
                    fileTreeMapB.remove(posAllelesComplementAndReverse);
                    // System.out.println("WOW alelos complementariosYreversos: " + positionA1A2Chr + " " +
                    // posAllelesComplementAndReverse);
                } else {
                    // Else means that fileTreeMapB does not contain this SNP or any of its variants.
                    // Therefore, we keep the one in fileTreeMapA
                    fileTreeMapC.put(positionA1A2Chr, lineA);
                    // System.out.println("WOW fileTreeMapB does not contain this SNP: " + positionA1A2Chr);
                }
                // contador++;
            }

            fileTreeMapA.clear();

            // Now we have to put in fileTreeMapC the rest of values that remain in fileTreeMapB.
            // We iterate the fileTreeMapB (the rest of the...)
            mySet = fileTreeMapB.entrySet();
            // Move next key and value of Map by iterator
            iter = mySet.iterator();
            while (iter.hasNext()) {
                // key=value separator this by Map.Entry to get key and value
                Entry<String, String> m = iter.next();
                // getKey is used to get key of Map
                positionA1A2Chr = (String) m.getKey();
                lineB = (String) m.getValue();
                // Then we have to store the value in fileTreeMapC
                fileTreeMapC.put(positionA1A2Chr, lineB);
                // contador++;
            }

            fileTreeMapB.clear();
            // System.out.println("\n[DEBUG] We have processed the chromosome " + chromoS + ". contador " + contador);

            // Finally we put the fileTreeMapC into the outputFile
            // We print the header which is the same always!.
            if (chromo == chrStart) {
                writer.write(header);
                writer.newLine();
            }

            String myLine = null;

            mySet = fileTreeMapC.entrySet();
            // Move next key and value of Map by iterator
            iter = mySet.iterator();
            while (iter.hasNext()) {
                // key=value separator this by Map.Entry to get key and value
                Entry<String, String> m = iter.next();
                // getKey is used to get key of Map
                myLine = (String) m.getValue();

                writer.write(myLine);
                writer.newLine();
            }

            writer.flush();

            fileTreeMapC.clear();
            // System.out.println("\n[DEBUG] We have stored snps from chromosome " + chromoS + " in the output file");

        } // End of for(int chromo=chrStart; chromo <=chrEnd; chromo++)

        writer.close();

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
        CombineCondensedFilesExecutor executor = new CombineCondensedFilesExecutor(filteredA, combinedCondensedFile, true, false);
        try {
            executor.execute(mafThreshold, infoThreshold, hweCohortThreshold, hweCasesThreshold, hweControlsThreshold);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot process A file", gte);
        }

        // Now with crh 23
        // If filteredA != filteredX then there is chr23 file (filteredX, therefore we have to include it in the
        // results.
        // Other wise, there is nothing to do.
        if (!filteredA.equals(filteredX)) {
            executor = new CombineCondensedFilesExecutor(filteredA, combinedCondensedFile, true, true);
            try {
                executor.execute(mafThreshold, infoThreshold, hweCohortThreshold, hweCasesThreshold, hweControlsThreshold);
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
        GenerateTopHitsAllExecutor executor = new GenerateTopHitsAllExecutor(resultsAFile, outputTopHitFile, true, false);
        try {
            executor.execute(pvaThres);
        } catch (GuidanceTaskException gte) {
            throw new GuidanceTaskException("[ERROR] Cannot process A file", gte);
        }

        // Now we have to see if we have to include results for Chr23 that come in the second input file (resultsBFile)
        // The way to know whether we have results for chr23 is by checking that resultsAFile is equal to resultsBFile.
        // If they are equal, the we do not have chr23. Other wise we have to results for chr23 y resultsBFile and we
        // have
        // to include it in the outputTopHitFile
        if (!resultsAFile.equals(resultsBFile)) {
            executor = new GenerateTopHitsAllExecutor(resultsAFile, outputTopHitFile, true, true);
            try {
                executor.execute(pvaThres);
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

        // We read each line of th reducedFileA and put them into fileAList array of Strings
        TreeMap<String, ArrayList<String>> fileATreeMap = new TreeMap<String, ArrayList<String>>();
        Hashtable<String, Integer> reduceFileAHashTableIndex = null;
        Hashtable<Integer, String> reduceFileAHashTableIndexReversed = null;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(reduceFileA))))) {
            // Read header and avoid it
            String line = br.readLine();

            // Construct hashes with default header
            String newHeader = chrS.equals(ChromoInfo.MAX_NUMBER_OF_CHROMOSOMES_STR) ? HEADER_MIXED_X : HEADER_MIXED;
            // By default values for indexes in the header
            int indexPosition = 1;
            int indexRsId = 2;

            reduceFileAHashTableIndex = HashUtils.createHashWithHeader(newHeader, "\t");
            reduceFileAHashTableIndexReversed = HashUtils.createHashWithHeaderReversed(newHeader, "\t");
            indexPosition = reduceFileAHashTableIndex.get("position");
            indexRsId = reduceFileAHashTableIndex.get("rs_id_all");

            // Process file
            while ((line = br.readLine()) != null) {
                ArrayList<String> fileAList = new ArrayList<>();
                // delimiter I assume a tap.
                String[] splitted = line.split("\t");

                // Store Position:Store rsIDCases:Store infoCases:Store certCases
                fileAList.add(line);
                String positionAndRsId = splitted[indexPosition] + "_" + splitted[indexRsId];

                // We only store the ones that are not repeated.
                // Question for Siliva: what is the criteria?
                if (!fileATreeMap.containsKey(positionAndRsId)) {
                    // Now, we put this casesList into the treemap with the key position
                    fileATreeMap.put(positionAndRsId, fileAList);
                } else {
                    fileATreeMap.remove(positionAndRsId);
                }
            }
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot process fileA", ioe);
        }

        // We read each line of th reducedFileB and put them into fileAList array of Strings
        TreeMap<String, ArrayList<String>> fileBTreeMap = new TreeMap<String, ArrayList<String>>();
        Hashtable<String, Integer> reduceFileBHashTableIndex = null;
        Hashtable<Integer, String> reduceFileBHashTableIndexReversed = null;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(reduceFileB))))) {
            // Read header and avoid it
            String line = br.readLine();

            // Construct hashes with default header
            String newHeader = chrS.equals(ChromoInfo.MAX_NUMBER_OF_CHROMOSOMES_STR) ? HEADER_MIXED_X : HEADER_MIXED;
            // By default values for indexes in the header
            int indexPosition = 1;
            int indexRsId = 2;

            reduceFileBHashTableIndex = HashUtils.createHashWithHeader(newHeader, "\t");
            reduceFileBHashTableIndexReversed = HashUtils.createHashWithHeaderReversed(newHeader, "\t");
            indexPosition = reduceFileBHashTableIndex.get("position");
            indexRsId = reduceFileBHashTableIndex.get("rs_id_all");

            // Process file
            while ((line = br.readLine()) != null) {
                ArrayList<String> fileBList = new ArrayList<>();
                // delimiter I assume a tap.
                String[] splitted = line.split("\t");

                // Store Position:Store rsIDCases:Store infoCases:Store certCases
                fileBList.add(line);
                String positionAndRsId = splitted[indexPosition] + "_" + splitted[indexRsId];

                // We only store the ones that are not repeated.
                // Question for Siliva: what is the criteria?
                if (!fileATreeMap.containsKey(positionAndRsId)) {
                    // Now, we put this casesList into the treemap with the key position
                    fileATreeMap.put(positionAndRsId, fileBList);
                } else {
                    fileATreeMap.remove(positionAndRsId);
                }
            }
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot process fileA", ioe);
        }

        // A place to store the results of this merge
        TreeMap<String, ArrayList<String>> fileCTreeMap = new TreeMap<>();
        Set<Entry<String, ArrayList<String>>> mySet = fileATreeMap.entrySet();
        // Move next key and value of Map by iterator
        Iterator<Entry<String, ArrayList<String>>> iter = mySet.iterator();
        while (iter.hasNext()) {
            // key=value separator this by Map.Entry to get key and value
            Entry<String, ArrayList<String>> m = iter.next();
            // getKey is used to get key of Map
            // position=(Integer)m.getKey();
            String positionAndRsId = (String) m.getKey();

            // getValue is used to get value of key in Map
            ArrayList<String> fileTmp = m.getValue();
            // We look for the casesPosition key in the controlsTreeMap.
            // If found, we get the value, otherwise we get null

            fileCTreeMap.put(positionAndRsId, fileTmp);
        }
        mySet = fileBTreeMap.entrySet();
        // Move next key and value of Map by iterator
        iter = mySet.iterator();
        while (iter.hasNext()) {
            // key=value separator this by Map.Entry to get key and value
            Entry<String, ArrayList<String>> m = iter.next();
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
        Hashtable<Integer, String> reversedTable = (reduceFileAHashTableIndexReversed.size() >= reduceFileBHashTableIndexReversed.size())
                ? reduceFileAHashTableIndexReversed : reduceFileBHashTableIndexReversed;
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

        // Indexes for impute 2.3.2
        /*
         * int indexRsId = 1; int indexPosition = 2; int indexInfo = 6; int indexCertainty= 7;
         */
        int indexRsId = 0;
        int indexPosition = 0;
        int indexInfo = 0;
        int indexCertainty = 0;
        int length_entry_assoc_list = 0;

        // genOrBimFile is the mixedGenFile and is not necessary to process it.

        // A place to store the results of the merge
        TreeMap<String, ArrayList<String>> summaryTotal = new TreeMap<>();
        Hashtable<String, Integer> snptestHashTableIndex = new Hashtable<>();
        Hashtable<Integer, String> snptestHashTableIndexReversed = new Hashtable<>();

        // We read each line of the firstImputeFileInfo and put them into firstList array of Strings
        TreeMap<String, ArrayList<String>> firstTreeMap = new TreeMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(firstImputeFileInfo))) {
            // Read the header and avoid the header
            String header = br.readLine();
            Hashtable<String, Integer> imputeHashTableIndex = new Hashtable<>();
            if (header != null && !header.isEmpty()) {
                // System.err.println("[collectSummary] IMPRIMO line: " + line);
                // If we are here, the file is not empty.
                imputeHashTableIndex = HashUtils.createHashWithHeader(header, " ");

                indexPosition = imputeHashTableIndex.get("position");
                indexRsId = imputeHashTableIndex.get("rs_id");
                indexInfo = imputeHashTableIndex.get("info");
                indexCertainty = imputeHashTableIndex.get("certainty");

                // System.out.println("indexPosition: "+indexPosition);
                // System.out.println("indexRsId: " + indexRsId);
                // System.out.println("indexInfo: " + indexInfo);
                // System.out.println("indexCertainty: " + indexCertainty);
            }

            String line = null;
            String positionAndRsId = null;
            while ((line = br.readLine()) != null) {
                ArrayList<String> firstList = new ArrayList<>();
                // delimiter I assume single space.
                String[] splitted = line.split(" ");

                // Store Position:Store rsIDCases:Store infoCases:Store certCases
                firstList.add(splitted[indexPosition]);
                firstList.add(splitted[indexRsId]);
                firstList.add(splitted[indexInfo]);
                firstList.add(splitted[indexCertainty]);
                positionAndRsId = splitted[indexPosition] + "_" + splitted[indexRsId];

                // If there is not a previous snp with this combination of position and rsID, we store it.
                if (!firstTreeMap.containsKey(positionAndRsId)) {
                    // We, put this in the firstTreeMap
                    firstTreeMap.put(positionAndRsId, firstList);
                } else { // If there is a snp with this combination we should remove it.
                    firstTreeMap.remove(positionAndRsId);
                }
            }
        } catch (IOException ioe) {
            throw new GuidanceTaskException("[ERROR] Cannot read first impute file", ioe);
        }

        // We read each line of the snptestOutFile and put them into assocTreeMap array of Strings
        TreeMap<String, ArrayList<String>> assocTreeMap = new TreeMap<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(snptestOutFile))))) {
            String line = null;
            String positionAndRsId = null;
            while ((line = br.readLine()) != null) {
                // We have to avoid reading all the comment lines that start with "#" character, and one more that is
                // the
                // header and start with "alternate".
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
        // Get TreeMap in Set interface to get key and value
        Set<Entry<String, ArrayList<String>>> mySet = firstTreeMap.entrySet();
        // Move next key and value of Map by iterator
        Iterator<Entry<String, ArrayList<String>>> iter = mySet.iterator();
        while (iter.hasNext()) {
            // key=value separator this by Map.Entry to get key and value
            Entry<String, ArrayList<String>> m = iter.next();
            // getKey is used to get key of Map
            String firstPositionAndRsId = (String) m.getKey();

            // getValue is used to get value of key in Map
            ArrayList<String> firstTmp = new ArrayList<>();
            firstTmp = m.getValue();
            // The same for assocTreeMap. If found, we get the value, otherwise we get null
            ArrayList<String> assocTmp = new ArrayList<>();
            assocTmp = assocTreeMap.get(firstPositionAndRsId);
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

        // First, let's create the header for this file

        String headerPhenomeFile = "chr\tposition";
        // headerPhenomeFile = headerPhenomeFile + "\t" + ttName + ":" + rpName + ":" + "rs_id_all";
        // headerPhenomeFile = headerPhenomeFile + "\t" + ttName + ":" + rpName + ":" + "alleleA";
        // headerPhenomeFile = headerPhenomeFile + "\t" + ttName + ":" + rpName + ":" + "alleleB";
        // headerPhenomeFile = headerPhenomeFile + "\t" + ttName + ":" + rpName + ":" + "all_maf";
        // headerPhenomeFile = headerPhenomeFile + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_pvalue";
        // headerPhenomeFile = headerPhenomeFile + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_beta_1";
        // headerPhenomeFile = headerPhenomeFile + "\t" + ttName + ":" + rpName + ":" +
        // "frequentist_add_beta_1:genotype/sex=1";
        // headerPhenomeFile = headerPhenomeFile + "\t" + ttName + ":" + rpName + ":" +
        // "frequentist_add_beta_2:genotype/sex=2";
        // headerPhenomeFile = headerPhenomeFile + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_se_1";
        // headerPhenomeFile = headerPhenomeFile + "\t" + ttName + ":" + rpName + ":" +
        // "frequentist_add_se_1:genotype/sex=1";
        // headerPhenomeFile = headerPhenomeFile + "\t" + ttName + ":" + rpName + ":" +
        // "frequentist_add_se_2:genotype/sex=2";

        // Then, read the input file

        String chrAndPosition = null;
        // TODO HERE
        // replace commas in the header variable by a TAB
        String newHeader = headerPhenomeFile;
        // String newHeader = headerPhenomeFile.replace(',', '\t');
        if (DEBUG) {
            System.out.println("[DEBUG] \t- The new header will be : [" + newHeader + "]");
        }

        // First, we load the whole topHitsFile into a TreeMap

        GZIPInputStream topHitsFileGz = null;
        Reader decoder = null;
        BufferedReader br = null;

        topHitsFileGz = new GZIPInputStream(new FileInputStream(topHitsFile));
        decoder = new InputStreamReader(topHitsFileGz);
        br = new BufferedReader(decoder);

        // A place to store the results of the merge
        TreeMap<String, ArrayList<String>> phenomeTreeMap = new TreeMap<>();

        // Now we create the header for the phenomeFile. The label variable should contain all the information
        // to do it, as follows (//Everything in one line)
        // chr:position:rsId:pval_phe_[0]_pan_[0]:pval_phe_[0]_pa_[1]:...:pval_phe_[0]_pa_[K-1]:
        // pval_phe_[1]_pan_[0]:pval_phe_[1]_pa_[1]:...:pval_phe_[1]_pa_[K-1]:
        // pval_phe_[2]_pan_[0]:pval_phe_[2]_pa_[1]:...:pval_phe_[2]_pa_[K-1]:
        // ...
        // pval_phe_[T-1]_pan_[0]:pval_phe_[T-1]_pa_[1]:...:pval_phe_(T-1)_pa_[K-1]:
        // Where:
        // T : number of phenotypes.
        // phe_[i]: name of phenotype i.
        // K : number of panels.
        // pa_[j]: name of panel j.
        Hashtable<String, Integer> phenomeHashTableIndex = new Hashtable<>();
        Hashtable<Integer, String> phenomeHashTableIndexReversed = new Hashtable<>();

        phenomeHashTableIndex = HashUtils.createHashWithHeader(newHeader, "\t");

        // We start reading the topHits File
        String line = br.readLine();
        Hashtable<String, Integer> topHitsHashTableIndex = new Hashtable<String, Integer>();
        topHitsHashTableIndex = HashUtils.createHashWithHeader(line, "\t");

        int indexChrInTopHitsFile = topHitsHashTableIndex.get("chr");
        int indexPositionInTopHitsFile = topHitsHashTableIndex.get("position");

        chrAndPosition = null;
        while ((line = br.readLine()) != null) {
            ArrayList<String> firstList = new ArrayList<String>();
            // delimiter I assume TAP space.
            String[] splitted = line.split("\t");

            chrAndPosition = splitted[indexChrInTopHitsFile] + "_" + splitted[indexPositionInTopHitsFile];

            // Store chr:position:rsId:pvalues for the all combination of phenotypes and panels
            // It means
            firstList.add(splitted[indexChrInTopHitsFile]);
            firstList.add(splitted[indexPositionInTopHitsFile]);

            // System.out.println("\n[DEBUG] phenomeHashTableIndex.size() " + phenomeHashTableIndex.size());

            // Finally, we put this data into the firstTreeMap, using chrPosition as key and the firstList as value.
            phenomeTreeMap.put(chrAndPosition, firstList);
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

        // TODO HERE
        GZIPInputStream phenomeAFileGz = null;
        Reader decoder = null;
        BufferedReader br = null;

        phenomeAFileGz = new GZIPInputStream(new FileInputStream(phenomeAFile));
        decoder = new InputStreamReader(phenomeAFileGz);
        br = new BufferedReader(decoder);

        // A place to store the results
        TreeMap<String, ArrayList<String>> phenomeATreeMap = new TreeMap<>();
        TreeMap<String, ArrayList<String>> phenomeBTreeMap = new TreeMap<>();

        // Now we create the header for the phenomeFile. The label variable should contain all the information
        // to do it, as follows (//Everything in one line)
        // chr:position:rsId:pval_phe_[0]_pan_[0]:pval_phe_[0]_pa_[1]:...:pval_phe_[0]_pa_[K-1]:
        // pval_phe_[1]_pan_[0]:pval_phe_[1]_pa_[1]:...:pval_phe_[1]_pa_[K-1]:
        // pval_phe_[2]_pan_[0]:pval_phe_[2]_pa_[1]:...:pval_phe_[2]_pa_[K-1]:
        // ...
        // pval_phe_[T-1]_pan_[0]:pval_phe_[T-1]_pa_[1]:...:pval_phe_(T-1)_pa_[K-1]:
        // Where:
        // T : number of phenotypes.
        // phe_[i]: name of phenotype i.
        // K : number of panels.
        // pa_[j]: name of panel j.
        Hashtable<String, Integer> phenomeAHashTableIndex = new Hashtable<String, Integer>();

        // We start reading the phenomeFileA
        // First of all, the header
        String phenomeAHeader = br.readLine();

        phenomeAHeader = "chr\tposition";
        // phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "rs_id_all";
        // phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "alleleA";
        // phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "alleleB";
        // phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "all_maf";
        // phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_pvalue";
        // phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_beta_1";
        // headerPhenomeFile = headerPhenomeFile + "\t" + ttName + ":" + rpName + ":" +
        // "frequentist_add_beta_1:genotype/sex=1";
        // headerPhenomeFile = headerPhenomeFile + "\t" + ttName + ":" + rpName + ":" +
        // "frequentist_add_beta_2:genotype/sex=2";
        // phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_se_1";
        // headerPhenomeFile = headerPhenomeFile + "\t" + ttName + ":" + rpName + ":" +
        // "frequentist_add_se_1:genotype/sex=1";
        // headerPhenomeFile = headerPhenomeFile + "\t" + ttName + ":" + rpName + ":" +
        // "frequentist_add_se_2:genotype/sex=2";

        phenomeAHashTableIndex = HashUtils.createHashWithHeader(phenomeAHeader, "\t");

        int indexChrInPhenomeAFile = phenomeAHashTableIndex.get("chr");
        int indexPositionInPhenomeAFile = phenomeAHashTableIndex.get("position");

        // Then, we read the rest of the file and put the information into the phenomeATreeMap
        String chrAndPosition = null;
        String line = null;

        while ((line = br.readLine()) != null) {
            ArrayList<String> firstList = new ArrayList<String>();
            // delimiter,I assume TAP space.
            String[] splitted = line.split("\t");

            chrAndPosition = splitted[indexChrInPhenomeAFile] + "_" + splitted[indexPositionInPhenomeAFile];

            // Store chr:position
            firstList.add(splitted[indexChrInPhenomeAFile]);
            firstList.add(splitted[indexPositionInPhenomeAFile]);

            // Finally, we put this data into the phenomeATreeMap, using chrPosition as key and the firstList as value.
            phenomeATreeMap.put(chrAndPosition, firstList);
        }

        // Now we read the topHitsFile
        GZIPInputStream topHitsFileGz = null;

        topHitsFileGz = new GZIPInputStream(new FileInputStream(topHitsFile));
        decoder = new InputStreamReader(topHitsFileGz);
        br = new BufferedReader(decoder);

        // We start reading the topHits File
        line = br.readLine();
        Hashtable<String, Integer> topHitsHashTableIndex = new Hashtable<String, Integer>();
        topHitsHashTableIndex = HashUtils.createHashWithHeader(line, "\t");

        int indexChrInTopHitsFile = topHitsHashTableIndex.get("chr");
        int indexPositionInTopHitsFile = topHitsHashTableIndex.get("position");

        chrAndPosition = null;
        while ((line = br.readLine()) != null) {
            ArrayList<String> firstList = new ArrayList<String>();
            // delimiter I assume TAP space.
            String[] splitted = line.split("\t");

            chrAndPosition = splitted[indexChrInTopHitsFile] + "_" + splitted[indexPositionInTopHitsFile];

            if (!phenomeATreeMap.containsKey(chrAndPosition)) {
                firstList.add(splitted[indexChrInTopHitsFile]);
                firstList.add(splitted[indexPositionInTopHitsFile]);

                // Finally, we put this data into the phenomeATreeMap, using chrPosition as key and the firstList as
                // value.
                phenomeATreeMap.put(chrAndPosition, firstList);
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
        GZIPInputStream phenomeAFileGz = null;
        Reader decoder = null;
        BufferedReader br = null;

        phenomeAFileGz = new GZIPInputStream(new FileInputStream(phenomeAFile));
        decoder = new InputStreamReader(phenomeAFileGz);
        br = new BufferedReader(decoder);

        // A place to store the data
        TreeMap<String, ArrayList<String>> phenomeATreeMap = new TreeMap<String, ArrayList<String>>();

        Hashtable<String, Integer> phenomeAHashTableIndex = new Hashtable<>();
        Hashtable<Integer, String> phenomeAHashTableIndexReversed = new Hashtable<>();

        // We start reading the phenomeFileA
        // First of all, the header
        String phenomeAHeader = br.readLine();

        // phenomeAHeader = "chr\tposition";
        phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "rs_id_all";
        phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "alleleA";
        phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "alleleB";
        phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "all_maf";
        phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_pvalue";
        phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_beta_1";
        phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_se_1";

        phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_beta_1:genotype/sex=1";
        phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_beta_2:genotype/sex=2";
        phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_se_1:genotype/sex=1";
        phenomeAHeader = phenomeAHeader + "\t" + ttName + ":" + rpName + ":" + "frequentist_add_se_2:genotype/sex=2";

        phenomeAHashTableIndex = HashUtils.createHashWithHeader(phenomeAHeader, "\t");

        int indexChrInPhenomeAFile = phenomeAHashTableIndex.get("chr");
        int indexPositionInPhenomeAFile = phenomeAHashTableIndex.get("position");

        // int indexRsIdInPhenomeAFile = phenomeAHashTableIndex.get(ttName + ":" + rpName + ":" + "rs_id_all");
        // int indexAlleleAInPhenomeAFile = phenomeAHashTableIndex.get(ttName + ":" + rpName + ":" + "alleleA");
        // int indexAlleleBInPhenomeAFile = phenomeAHashTableIndex.get(ttName + ":" + rpName + ":" + "alleleB");
        // int indexallMAFInPhenomeAFile = phenomeAHashTableIndex.get(ttName + ":" + rpName + ":" + "all_maf");
        // int indexFreqAddPvalueInPhenomeAFile = phenomeAHashTableIndex.get(ttName + ":" + rpName + ":" +
        // "frequentist_add_pvalue");
        // int indexFreqAddBetaInPhenomeAFile = phenomeAHashTableIndex.get(ttName + ":" + rpName + ":" +
        // "frequentist_add_beta_1");
        // int indexFreqAddSeInPhenomeAFile = phenomeAHashTableIndex.get(ttName + ":" + rpName + ":" +
        // "frequentist_add_se_1");

        // int indexFreqAddBeta1Sex1InPhenomeAFile = phenomeAHashTableIndex
        // .get(ttName + ":" + rpName + ":" + "frequentist_add_beta_1:genotype/sex=1");
        // int indexFreqAddBeta2Sex2InPhenomeAFile = phenomeAHashTableIndex
        // .get(ttName + ":" + rpName + ":" + "frequentist_add_beta_2:genotype/sex=2");
        // int indexFreqAddSe1Sex1InPhenomeAFile = phenomeAHashTableIndex
        // .get(ttName + ":" + rpName + ":" + "frequentist_add_se_1:genotype/sex=1");
        // int indexFreqAddSe2Sex2InPhenomeAFile = phenomeAHashTableIndex
        // .get(ttName + ":" + rpName + ":" + "frequentist_add_se_2:genotype/sex=2");

        // --->
        // Then, we read the rest of the file and put the information into the phenomeATreeMap
        String chrAndPosition = null;
        String line = null;
        while ((line = br.readLine()) != null) {
            ArrayList<String> currentList = new ArrayList<String>();
            // delimiter,I assume TAP space.
            String[] splitted = line.split("\t");

            chrAndPosition = splitted[indexChrInPhenomeAFile] + "_" + splitted[indexPositionInPhenomeAFile];

            // currentList.add(splitted[indexChrInPhenomeAFile]);
            // currentList.add(splitted[indexPositionInPhenomeAFile]);
            for (int i = 0; i < splitted.length; i++) {
                currentList.add(splitted[i]);
            }

            // We update the phenomeATreeMap with the currentList
            phenomeATreeMap.put(chrAndPosition, currentList);
        }
        ////

        // Then, we need to extract the information of each snp from the filteredByAllFile
        // Now we load the whole filteredByAllFile into a TreeMap
        GZIPInputStream filteredByAllGz = null;
        decoder = null;
        br = null;

        filteredByAllGz = new GZIPInputStream(new FileInputStream(filteredByAllFile));
        decoder = new InputStreamReader(filteredByAllGz);
        br = new BufferedReader(decoder);

        TreeMap<String, ArrayList<String>> filteredTreeMap = new TreeMap<String, ArrayList<String>>();

        Hashtable<String, Integer> filteredByAllHashTableIndex = new Hashtable<String, Integer>();
        Hashtable<Integer, String> filteredByAllHashTableIndexReversed = new Hashtable<Integer, String>();

        // We start reading the filteredByAllFile
        // First of all, the header
        String filteredHeader = br.readLine();
        filteredByAllHashTableIndex = HashUtils.createHashWithHeader(filteredHeader, "\t");

        int indexChrInFiltered = filteredByAllHashTableIndex.get("chr");
        int indexPositionInFiltered = filteredByAllHashTableIndex.get("position");
        int indexRsIdInFiltered = filteredByAllHashTableIndex.get("rs_id_all");
        int indexAlleleAInFiltered = filteredByAllHashTableIndex.get("alleleA");
        int indexAlleleBInFiltered = filteredByAllHashTableIndex.get("alleleB");
        int indexAllMafInFiltered = filteredByAllHashTableIndex.get("all_maf");
        int indexFreqAddPvalueInFiltered = filteredByAllHashTableIndex.get("frequentist_add_pvalue");
        int indexFreqAddBetaInFiltered = filteredByAllHashTableIndex.get("frequentist_add_beta_1");
        // int indexFreqAddBeta1sex1InFiltered =
        // filteredByAllHashTableIndex.get("frequentist_add_beta_1:genotype/sex=1");
        // int indexFreqAddBeta2sex2InFiltered =
        // filteredByAllHashTableIndex.get("frequentist_add_beta_2:genotype/sex=2");
        int indexFreqAddSeInFiltered = filteredByAllHashTableIndex.get("frequentist_add_se_1");
        // int indexFreqAddSe1sex1InFiltered = filteredByAllHashTableIndex.get("frequentist_add_se_1:genotype/sex=1");
        // int indexFreqAddSe2sex2InFiltered = filteredByAllHashTableIndex.get("frequentist_add_se_2:genotype/sex=2");

        line = null;
        while ((line = br.readLine()) != null) {
            String[] splitted = line.split("\t");
            chrAndPosition = splitted[indexChrInFiltered] + "_" + splitted[indexPositionInFiltered];

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

            filteredTreeMap.put(chrAndPosition, reducedList);
        }

        // Here we have to do some similar with filteredByAllXFile (the results for chr23)

        TreeMap<String, ArrayList<String>> filteredXTreeMap = new TreeMap<String, ArrayList<String>>();

        Hashtable<String, Integer> filteredByAllXHashTableIndex = new Hashtable<String, Integer>();
        Hashtable<Integer, String> filteredByAllXHashTableIndexReversed = new Hashtable<Integer, String>();

        if (endChrS.equals("23")) {
            // Then, we need to extract the information of each snp from the filteredByAllFile
            // Now we load the whole filteredByAllFile into a TreeMap
            GZIPInputStream filteredByAllXGz = null;
            decoder = null;
            br = null;

            filteredByAllXGz = new GZIPInputStream(new FileInputStream(filteredByAllXFile));
            decoder = new InputStreamReader(filteredByAllXGz);
            br = new BufferedReader(decoder);

            // We start reading the filteredByAllXFile
            // First of all, the header
            String filteredXHeader = br.readLine();
            filteredByAllXHashTableIndex = HashUtils.createHashWithHeader(filteredXHeader, "\t");

            int indexChrInFilteredX = filteredByAllXHashTableIndex.get("chr");
            int indexPositionInFilteredX = filteredByAllXHashTableIndex.get("position");
            int indexRsIdInFilteredX = filteredByAllXHashTableIndex.get("rs_id_all");
            int indexAlleleAInFilteredX = filteredByAllXHashTableIndex.get("alleleA");
            int indexAlleleBInFilteredX = filteredByAllXHashTableIndex.get("alleleB");
            int indexAllMafInFilteredX = filteredByAllXHashTableIndex.get("all_maf");
            int indexFreqAddPvalueInFilteredX = filteredByAllXHashTableIndex.get("frequentist_add_pvalue");
            // int indexFreqAddBetaInFilteredX = filteredByAllXHashTableIndex.get("frequentist_add_beta_1");
            // int indexFreqAddSeInFilteredX = filteredByAllXHashTableIndex.get("frequentist_add_se_1");

            int indexFreqAddBeta1sex1InFilteredX = filteredByAllXHashTableIndex.get("frequentist_add_beta_1:genotype/sex=1");
            int indexFreqAddBeta2sex2InFilteredX = filteredByAllXHashTableIndex.get("frequentist_add_beta_2:genotype/sex=2");
            int indexFreqAddSe1sex1InFilteredX = filteredByAllXHashTableIndex.get("frequentist_add_se_1:genotype/sex=1");
            int indexFreqAddSe2sex2InFilteredX = filteredByAllXHashTableIndex.get("frequentist_add_se_2:genotype/sex=2");

            line = null;
            while ((line = br.readLine()) != null) {
                String[] splitted = line.split("\t");
                chrAndPosition = splitted[indexChrInFilteredX] + "_" + splitted[indexPositionInFilteredX];

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

                filteredXTreeMap.put(chrAndPosition, reducedList);
            }
        }

        // Now, we print the information of each snp from filteredByAllFile into the phenomeATreeMap

        Set<Entry<String, ArrayList<String>>> mySet = phenomeATreeMap.entrySet();
        // Move next key and value of Map by iterator
        Iterator<Entry<String, ArrayList<String>>> iter = mySet.iterator();
        while (iter.hasNext()) {
            // key=value separator this by Map.Entry to get key and value
            Entry<String, ArrayList<String>> m = iter.next();

            ArrayList<String> currentList = m.getValue();
            ArrayList<String> reducedList = null;

            // getKey is used to get key of Map
            chrAndPosition = (String) m.getKey();

            String[] divideKey = chrAndPosition.split("_");
            String Chr = divideKey[0];

            if (!Chr.equals("23")) {
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

        // --------->
        // First we read the phenomeAFile and load the information into the phenomeATreeMap
        GZIPInputStream phenomeAFileGz = null;
        Reader decoder = null;
        BufferedReader br = null;

        phenomeAFileGz = new GZIPInputStream(new FileInputStream(phenomeAFile));
        decoder = new InputStreamReader(phenomeAFileGz);
        br = new BufferedReader(decoder);

        // A place to store the data
        TreeMap<String, ArrayList<String>> phenomeATreeMap = new TreeMap<String, ArrayList<String>>();

        Hashtable<String, Integer> phenomeAHashTableIndex = new Hashtable<String, Integer>();
        Hashtable<Integer, String> phenomeAHashTableIndexReversed = new Hashtable<Integer, String>();

        // We start reading the phenomeFileA
        // First of all, the header
        String phenomeAHeader = br.readLine();
        phenomeAHashTableIndex = HashUtils.createHashWithHeader(phenomeAHeader, "\t");

        int indexChrInPhenomeAFile = phenomeAHashTableIndex.get("chr");
        int indexPositionInPhenomeAFile = phenomeAHashTableIndex.get("position");

        // Then, we read the rest of the file and put the information into the phenomeATreeMap
        String chrAndPosition = null;
        String line = null;
        while ((line = br.readLine()) != null) {
            ArrayList<String> firstList = new ArrayList<String>();
            // delimiter,I assume TAP space.
            String[] splitted = line.split("\t");

            chrAndPosition = splitted[indexChrInPhenomeAFile] + "_" + splitted[indexPositionInPhenomeAFile];

            // Store chr:position
            for (int i = 0; i < splitted.length; i++) {
                firstList.add(splitted[i]);
            }
            // Finally, we put this data into the phenomeATreeMap, using chrPosition as key and the firstList as value.
            phenomeATreeMap.put(chrAndPosition, firstList);
        }

        // Second we read the phenomeBFile and load the information into the phenomeATreeMap
        decoder = null;
        br = null;

        GZIPInputStream phenomeBFileGz = new GZIPInputStream(new FileInputStream(phenomeBFile));
        decoder = new InputStreamReader(phenomeBFileGz);
        br = new BufferedReader(decoder);

        // A place to store the data
        TreeMap<String, ArrayList<String>> phenomeBTreeMap = new TreeMap<String, ArrayList<String>>();

        Hashtable<String, Integer> phenomeBHashTableIndex = new Hashtable<String, Integer>();
        Hashtable<Integer, String> phenomeBHashTableIndexReversed = new Hashtable<Integer, String>();

        // We start reading the phenomeFileA
        // First of all, the header
        String phenomeBHeader = br.readLine();
        phenomeBHashTableIndex = HashUtils.createHashWithHeader(phenomeBHeader, "\t");
        phenomeBHashTableIndexReversed = HashUtils.createHashWithHeaderReversed(phenomeBHeader, "\t");

        int indexChrInPhenomeBFile = phenomeBHashTableIndex.get("chr");
        int indexPositionInPhenomeBFile = phenomeBHashTableIndex.get("position");

        // Then, we read the rest of the file and put the information into the phenomeATreeMap
        chrAndPosition = null;
        line = null;

        while ((line = br.readLine()) != null) {
            ArrayList<String> firstList = new ArrayList<String>();
            // delimiter,I assume TAP space.
            String[] splitted = line.split("\t");

            chrAndPosition = splitted[indexChrInPhenomeBFile] + "_" + splitted[indexPositionInPhenomeBFile];

            if (phenomeATreeMap.containsKey(chrAndPosition)) {
                firstList = phenomeATreeMap.get(chrAndPosition);

                for (int i = 2; i < splitted.length; i++) {
                    firstList.add(splitted[i]);
                }

                // Finally, we put this data into the phenomeATreeMap, using chrPosition as key and the firstList as
                // value.
                phenomeATreeMap.put(chrAndPosition, firstList);
            }
        }

        String finalHeader = phenomeAHeader;
        for (int i = 2; i < phenomeBHashTableIndex.size(); i++) {
            finalHeader = finalHeader + "\t" + phenomeBHashTableIndexReversed.get(i);
        }

        // Finally, we print the phenomeAThreeMap into the output file
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
