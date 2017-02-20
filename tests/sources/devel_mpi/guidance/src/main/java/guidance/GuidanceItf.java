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
 *  Last update: $LastChangedDate: 2015-02-17 11:26:34 +0100 (Tue, 17 Feb 2015) $
 *  Revision Number: $Revision: 23 $
 *  Last revision  : $LastChangedRevision: 23 $
 *  Written by     : Friman Sanchez C.
 *                 : friman.sanchez@gmail.com
 *  Modified by    :
 *                
 *  Guidance web page: http://cg.bsc.es/guidance/
 */

package guidance;

import guidance.utils.Environment;
import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.task.Binary;
import integratedtoolkit.types.annotations.task.Method;
import integratedtoolkit.types.annotations.Parameter;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Stream;
import integratedtoolkit.types.annotations.parameter.Type;

public interface GuidanceItf {
    
    // TODO: WARN
    // Task getAllele from GuidanceImpl is not defined as task
    
    @Binary(binary = "${" + Environment.EV_PLINKBINARY + "}")
    @Constraints(computingUnits = "1", memorySize = "1.0")
    Integer convertFromBedToBed(
        @Parameter(type = Type.STRING, direction = Direction.IN) String noWebFlag, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String bedPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String bedFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String bimPrefix,
        @Parameter(type = Type.FILE, direction = Direction.IN) String bimFile,
        @Parameter(type = Type.STRING, direction = Direction.IN) String famPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String famFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String chrPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String chromo, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String recodeFlag, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String outPrefix, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String newBedFileName,
        @Parameter(type = Type.STRING, direction = Direction.IN) String makeBedFlag, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, prefix = "#") String newBedFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, prefix = "#") String newBimFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, prefix = "#") String newFamFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, prefix = "#") String newLogFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String stdout, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDERR) String stderr
    );
    

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "1.0f")
   void convertFromBedToBed(
      @Parameter(type = Type.FILE,   direction = Direction.IN)  String bedFile,
      @Parameter(type = Type.FILE,   direction = Direction.IN) String bimFile,
      @Parameter(type = Type.FILE,   direction = Direction.IN) String famFile,
      @Parameter(type = Type.FILE,   direction = Direction.OUT) String newBedFile,
      @Parameter(type = Type.FILE,   direction = Direction.OUT) String newBimFile,
      @Parameter(type = Type.FILE,   direction = Direction.OUT) String newFamFile,
      @Parameter(type = Type.FILE,   direction = Direction.OUT) String logFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String chromo,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "1.0f")
   void createRsIdList(
      @Parameter(type = Type.FILE,   direction = Direction.IN)  String genFile,
      @Parameter(type = Type.STRING, direction = Direction.IN)  String exclCgatFlag,
      @Parameter(type = Type.FILE,   direction = Direction.OUT) String pairsFile,
      @Parameter(type = Type.STRING, direction = Direction.IN)  String inputFormat,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "2.0f")
   void qctoolS(
      @Parameter(type = Type.FILE, direction = Direction.IN) String imputeFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String inclusionRsIdFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String mafThresholdS,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String filteredFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String filteredLogFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "4.0f")
   void createListOfExcludedSnps(
      @Parameter(type = Type.FILE, direction = Direction.IN) String shapeitHapsFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String excludedSnpsFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String exclCgatFlag,
      @Parameter(type = Type.STRING, direction = Direction.IN) String exclSVFlag,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );
    
   @Binary(binary = "${SHAPEITBINARY}")
   @Constraints(computingUnits="1", memorySize = "4.0f")
   void filterHaplotypes(
       @Parameter(type = Type.STRING, direction = Direction.IN) String convertFlag, 
       @Parameter(type = Type.STRING, direction = Direction.IN) String inputHapsPrefix, 
       @Parameter(type = Type.FILE, direction = Direction.IN) String hapsFile, 
       @Parameter(type = Type.FILE, direction = Direction.IN) String sampleFile,
       @Parameter(type = Type.STRING, direction = Direction.IN) String exludeSnpPrefix, 
       @Parameter(type = Type.FILE, direction = Direction.IN) String excludedSnpsFile, 
       @Parameter(type = Type.STRING, direction = Direction.IN) String outputHapsPrefix, 
       @Parameter(type = Type.FILE, direction = Direction.OUT) String filteredHapsFile, 
       @Parameter(type = Type.FILE, direction = Direction.OUT) String filteredSampleFile,
       @Parameter(type = Type.STRING, direction = Direction.IN) String outputLogPrefix, 
       @Parameter(type = Type.FILE, direction = Direction.OUT) String filteredLogFile, 
       @Parameter(type = Type.STRING, direction = Direction.IN) String outputVcfPrefix, 
       @Parameter(type = Type.FILE, direction = Direction.OUT) String filteredHapsVcfFile
   );
   
   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "4.0f")
   void postFilterHaplotypes(
       @Parameter(type = Type.FILE, direction = Direction.IN) String filteredHapsFile, 
       @Parameter(type = Type.FILE, direction = Direction.OUT) String listOfSnpsFile
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="16", memorySize = "20.0f")
   void phasing(
      @Parameter(type = Type.STRING, direction = Direction.IN) String chromo,
      @Parameter(type = Type.FILE, direction = Direction.IN) String gtoolGenFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String gtoolSampleFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String gmapFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String shapeitHapsFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String shapeitSampleFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String shapeitLogFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Binary(binary = "${SHAPEITBINARY}")
   @Constraints(computingUnits="16", memorySize = "20.0f")
   void phasingBed(
      @Parameter(type = Type.STRING, direction = Direction.IN) String inputBedPrefix,
      @Parameter(type = Type.FILE, direction = Direction.IN) String bedFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String bimFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String famFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String inputMapPrefix,
      @Parameter(type = Type.FILE, direction = Direction.IN) String gmapFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String chrXPrefix,
      @Parameter(type = Type.STRING, direction = Direction.IN) String outputMaxPrefix,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String shapeitHapsFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String shapeitSampleFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String threadPrefix,
      @Parameter(type = Type.INT, direction = Direction.IN) int numThreads,
      @Parameter(type = Type.STRING, direction = Direction.IN) String effectiveSizePrefix,
      @Parameter(type = Type.INT, direction = Direction.IN) int effectiveSize,
      @Parameter(type = Type.STRING, direction = Direction.IN) String outputLogPrefix,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String shapeitLogFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String shapeitStdOut,
      @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDERR) String shapeitStdErr
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "14.0f")
   void imputeWithImpute(
      @Parameter(type = Type.FILE, direction = Direction.IN) String gmapFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String knownHapFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String legendFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String shapeitHapsFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String shapeitSampleFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String lim1S,
      @Parameter(type = Type.STRING, direction = Direction.IN) String lim2S,
      @Parameter(type = Type.FILE, direction = Direction.IN) String pairsFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String imputeFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String imputeFileInfo,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String imputeFileSummary,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String imputeFileWarnings,
      @Parameter(type = Type.STRING, direction = Direction.IN) String theChromo,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );


   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "3.0f")
   void imputeWithMinimac(
      @Parameter(type = Type.FILE, direction = Direction.IN) String knownHapFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String filteredHapsFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String filteredSampleFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String filteredListOfSnpsFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String imputedMMFileName,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String imputedMMInfoFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String imputedMMErateFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String imputedMMRecFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String imputedMMDoseFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String imputedMMLogFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String theChromo,
      @Parameter(type = Type.STRING, direction = Direction.IN) String lim1S,
      @Parameter(type = Type.STRING, direction = Direction.IN) String lim2S,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "6.0f")
   void filterByAll(
      @Parameter(type = Type.FILE, direction = Direction.IN) String inputFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String outputFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String outputCondensedFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String mafThresholdS,
      @Parameter(type = Type.STRING, direction = Direction.IN) String infoThresholdS,
      @Parameter(type = Type.STRING, direction = Direction.IN) String hweCohortThresholdS,
      @Parameter(type = Type.STRING, direction = Direction.IN) String hweCasesThreshold,
      @Parameter(type = Type.STRING, direction = Direction.IN) String mafControlsThreshold,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "6.0f")
   void filterByInfo(
      @Parameter(type = Type.FILE, direction = Direction.IN) String imputeFileInfo,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String filteredFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String threshold,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "6.0f")
   void jointFilteredByAllFiles(
      @Parameter(type = Type.FILE, direction = Direction.IN) String filteredByAllA,
      @Parameter(type = Type.FILE, direction = Direction.IN) String filteredByAllB,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String filteredByAllC,
      @Parameter(type = Type.STRING, direction = Direction.IN) String rpanelName,
      @Parameter(type = Type.STRING, direction = Direction.IN) String rpanelFlag,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "6.0f")
   void jointCondensedFiles(
      @Parameter(type = Type.FILE, direction = Direction.IN) String inputAFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String inputBFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String outputFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "14.0f")
   void generateQQManhattanPlots(
      @Parameter(type = Type.FILE, direction = Direction.IN) String lastCondensedFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String qqPlotFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String manhattanPlotFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String qqPlotTiffFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String manhattanPlotTiffFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String correctedPvaluesFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "2.0f")
   void snptest(
      @Parameter(type = Type.FILE, direction = Direction.IN) String mergedGenFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String mergedSampleFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String snptestOutFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String snptestLogFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String responseVar,
      @Parameter(type = Type.STRING, direction = Direction.IN) String covariables,
      @Parameter(type = Type.STRING, direction = Direction.IN) String theChromo,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "8.0f")
   void collectSummary(
      @Parameter(type = Type.STRING, direction = Direction.IN) String chr,
      @Parameter(type = Type.FILE, direction = Direction.IN) String firstImputeFileInfo,
      @Parameter(type = Type.FILE, direction = Direction.IN) String snptestOutFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String reduceFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String mafThresholdS, 
      @Parameter(type = Type.STRING, direction = Direction.IN) String infoThresholdS, 
      @Parameter(type = Type.STRING, direction = Direction.IN) String hweCohortThresholdS,
      @Parameter(type = Type.STRING, direction = Direction.IN) String hweCasesThresholdS,
      @Parameter(type = Type.STRING, direction = Direction.IN) String hweControlsThresholdS,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "8.0f")
   void initPhenoMatrix(
      @Parameter(type = Type.FILE, direction = Direction.IN) String topHitsFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String ttName,
      @Parameter(type = Type.STRING, direction = Direction.IN) String rpName,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String phenomeFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "8.0f")
   void addToPhenoMatrix(
      @Parameter(type = Type.FILE, direction = Direction.IN) String phenomeAFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String topHitsFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String ttName,
      @Parameter(type = Type.STRING, direction = Direction.IN) String rpName,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String phenomeBFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "8.0f")
   void filloutPhenoMatrix(
      @Parameter(type = Type.FILE, direction = Direction.IN) String phenomeAFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String filteredByAllFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String filteredByAllXFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String thereisX,
      @Parameter(type = Type.STRING, direction = Direction.IN) String ttName,
      @Parameter(type = Type.STRING, direction = Direction.IN) String rpName,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String phenomeBFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "8.0f")
   void finalizePhenoMatrix(
      @Parameter(type = Type.FILE, direction = Direction.IN) String phenomeAFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String phenomeBFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String ttName,
      @Parameter(type = Type.STRING, direction = Direction.IN) String rpName,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String phenomeCFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "8.0f")
   void mergeTwoChunks(
      @Parameter(type = Type.FILE, direction = Direction.IN) String reduceFileA,
      @Parameter(type = Type.FILE, direction = Direction.IN) String reduceFileB,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String reduceFileC,
      @Parameter(type = Type.STRING, direction = Direction.IN) String chrS,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "20.0f")
   void combinePanelsComplex(
      @Parameter(type = Type.FILE, direction = Direction.IN) String resultsFileA,
      @Parameter(type = Type.FILE, direction = Direction.IN) String resultsFileB,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String resultsFileC,
      @Parameter(type = Type.STRING, direction = Direction.IN) String chromoStart,
      @Parameter(type = Type.STRING, direction = Direction.IN) String chromoEnd,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "6.0f")
   void combineCondensedFiles(
      @Parameter(type = Type.FILE, direction = Direction.IN) String filteredA,
      @Parameter(type = Type.FILE, direction = Direction.IN) String filteredX,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String combinedCondensedFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String mafThresholdS,
      @Parameter(type = Type.STRING, direction = Direction.IN) String infoThresholdS,
      @Parameter(type = Type.STRING, direction = Direction.IN) String hweCohortThresholdS,
      @Parameter(type = Type.STRING, direction = Direction.IN) String hweCasesThreshold,
      @Parameter(type = Type.STRING, direction = Direction.IN) String mafControlsThreshold,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

   @Method(declaringClass = "guidance.GuidanceImpl")
   @Constraints(computingUnits="1", memorySize = "8.0f")
   void generateTopHitsAll(
      @Parameter(type = Type.FILE, direction = Direction.IN) String resultsAFile,
      @Parameter(type = Type.FILE, direction = Direction.IN) String resultsBFile,
      @Parameter(type = Type.FILE, direction = Direction.OUT) String outputTopHitsFile,
      @Parameter(type = Type.STRING, direction = Direction.IN) String pvaThreshold,
      @Parameter(type = Type.STRING, direction = Direction.IN) String cmdToStore
   );

}
