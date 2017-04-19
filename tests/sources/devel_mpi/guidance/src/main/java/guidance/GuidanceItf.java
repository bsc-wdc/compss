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
    
    @Binary(binary = "$" + Environment.EV_PLINKBINARY )
    @Constraints(computingUnits = "1", memorySize = "1.0")
    Integer convertFromBedToBed(
        @Parameter(type = Type.STRING, direction = Direction.IN) String webFlag, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String bedPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String bedFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String bimPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String bimFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String famPrefix,
        @Parameter(type = Type.FILE, direction = Direction.IN) String famFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String chromoPrefix, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String chromo, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String recodeFlag, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String outPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.OUT)String newBedFile,
        @Parameter(type = Type.STRING, direction = Direction.IN) String makeBedFlag, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, prefix = "#") String newBimFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, prefix = "#") String newFamFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, prefix = "#") String newLogFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String stdOutputFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDERR) String stdErrorFile
    );
    
    @Binary(binary = "$" + Environment.EV_SHAPEITBINARY)
    @Constraints(computingUnits="16", memorySize = "20.0f")
    Integer phasingBed(
        @Parameter(type = Type.STRING, direction = Direction.IN) String inputBedPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String bedFile, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String bumFile, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String famFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String inputMapPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String gmapFile,
        @Parameter(type = Type.STRING, direction = Direction.IN) String chromoFlag, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String outputPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String shapeitHapsFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String shapeitSampleFile,
        @Parameter(type = Type.STRING, direction = Direction.IN) String threadPrefix, 
        @Parameter(type = Type.INT, direction = Direction.IN) int numThreads, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String effectiveSizePrefix, 
        @Parameter(type = Type.INT, direction = Direction.IN) int effectiveSize, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String outputLogPrefix,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String shapeitLogFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String stdOutputFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDERR) String stdErrorFile
    );
    
    @Binary(binary = "$" + Environment.EV_SHAPEITBINARY)
    @Constraints(computingUnits="16", memorySize = "20.0f")
    Integer phasing(
        @Parameter(type = Type.STRING, direction = Direction.IN) String inputGenPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String inputGenFile, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String inputSampleFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String inputMapPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String gmapFile,
        @Parameter(type = Type.STRING, direction = Direction.IN) String chromoFlag, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String outputPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String shapeitHapsFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String shapeitSampleFile,
        @Parameter(type = Type.STRING, direction = Direction.IN) String threadPrefix, 
        @Parameter(type = Type.INT, direction = Direction.IN) int numThreads, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String effectiveSizePrefix, 
        @Parameter(type = Type.INT, direction = Direction.IN) int effectiveSize, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String outputLogPrefix,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String shapeitLogFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String stdOutFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDERR) String stdErrorFile
    );

    @Binary(binary = "$" + Environment.EV_IMPUTE2BINARY)
    @Constraints(computingUnits="1", memorySize = "14.0f")
    Integer imputeWithImpute(
        @Parameter(type = Type.STRING, direction = Direction.IN) String prephasedFlag, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String mPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String gmapFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String hPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String knownHapFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String lPrefix,
        @Parameter(type = Type.FILE, direction = Direction.IN) String legendFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String knownHapsPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String shapeitHapsFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String samplePrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String shapeitSampleFile,
        @Parameter(type = Type.STRING, direction = Direction.IN) String intPrefix,
        @Parameter(type = Type.STRING, direction = Direction.IN) String lim1S, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String lim2S, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String chromoFlag, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String exludeSnpsPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String pairsFile,
        @Parameter(type = Type.STRING, direction = Direction.IN) String imputeExcludedFlag, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String nePrefix, 
        @Parameter(type = Type.INT, direction = Direction.IN) int ne, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String oPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String imputeFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String iPrefix,
        @Parameter(type = Type.FILE, direction = Direction.OUT) String imputeFileInfo, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String rPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String imputeFileSummary, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String wPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String imputeFileWarnings,
        @Parameter(type = Type.STRING, direction = Direction.IN) String noSampleQCFlag, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String oGzFlag, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String stdOutFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDERR) String stdErrorFile
    );
    
    @Binary(binary = "$" + Environment.EV_MINIMACBINARY)
    @Constraints(computingUnits="1", memorySize = "3.0f")
    Integer imputeWithMinimac(
        @Parameter(type = Type.STRING, direction = Direction.IN) String vcfReferenceFlag, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String refHapsPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String knownHapFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String snpsPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String filteredListOfSnpsFile,
        @Parameter(type = Type.STRING, direction = Direction.IN) String shapeHapsPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String filteredHapsFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String samplePrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String filteredSampleFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String vcfStartPrefix,
        @Parameter(type = Type.STRING, direction = Direction.IN) String lim1S,
        @Parameter(type = Type.STRING, direction = Direction.IN) String vcfEndPrefix, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String lim2S, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String chromoFlag, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String chromo, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String vcfWindowPrefix,
        @Parameter(type = Type.INT, direction = Direction.IN) int vcfWindow, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String roundsPrefix, 
        @Parameter(type = Type.INT, direction = Direction.IN) int rounds, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String statesPrefix, 
        @Parameter(type = Type.INT, direction = Direction.IN) int states, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String prefixPrefix, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String imputedMMFileName,
        @Parameter(type = Type.STRING, direction = Direction.IN) String gzipFlag, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, prefix = "#") String imputedMMInfoFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, prefix = "#") String imputedMMErateFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, prefix = "#") String imputedMMRecFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, prefix = "#") String imputedMMDoseFile,
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String stdOutFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDERR) String stdErrFile
    );
    
    @Method(declaringClass = "guidance.GuidanceImpl")
    @Constraints(computingUnits="1", memorySize = "14.0f")
    void preGenerateQQManhattanPlots(
        @Parameter(type = Type.FILE, direction = Direction.IN) String condensedFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String condensedFileUnzip
    );
    
    @Binary(binary = "$" + Environment.EV_RSCRIPTBINDIR + "/Rscript" + Environment.EV_RSCRIPTDIR + "/qqplot_manhattan.R")
    @Constraints(computingUnits="1", memorySize = "14.0f")    
    Integer generateQQManhattanPlots(
        @Parameter(type = Type.FILE, direction = Direction.IN) String inputFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String qqPlotFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String manhattanPlotFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String qqPlotTiffFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String manhattanPlotTiffFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String correctedPvaluesFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String stdOutFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDERR) String stdErrorFile
    );
    
    @Binary(binary = "$" + Environment.EV_SNPTESTBINARY) 
    @Constraints(computingUnits="1", memorySize = "2.0f")
    Integer snptest(
            @Parameter(type = Type.STRING, direction = Direction.IN) String dataPrefix, 
            @Parameter(type = Type.FILE, direction = Direction.IN) String mergedGenFile, 
            @Parameter(type = Type.FILE, direction = Direction.IN) String mergedSampleFile, 
            @Parameter(type = Type.STRING, direction = Direction.IN) String oPrefix, 
            @Parameter(type = Type.FILE, direction = Direction.OUT) String snpTestOutFile, 
            @Parameter(type = Type.STRING, direction = Direction.IN) String phenoPrefix,
            @Parameter(type = Type.STRING, direction = Direction.IN) String responseVar, 
            @Parameter(type = Type.STRING, direction = Direction.IN) String covarsFlag, 
            @Parameter(type = Type.STRING, direction = Direction.IN) String hweFlag, 
            @Parameter(type = Type.STRING, direction = Direction.IN) String logPrefix, 
            @Parameter(type = Type.FILE, direction = Direction.OUT) String snpTestLogFile,
            @Parameter(type = Type.STRING, direction = Direction.IN) String chromoFlag, 
            @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String stdOutFile, 
            @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDERR) String stdErrorFile
    );
    
    @Method(declaringClass = "guidance.GuidanceImpl")
    @Constraints(computingUnits="1", memorySize = "2.0f")
    void postSnptest(
        @Parameter(type = Type.FILE, direction = Direction.IN) String snpTestOutFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String snpTestOutFileZip
    );
    
    @Binary(binary = "$" + Environment.EV_QCTOOLBINARY)
    @Constraints(computingUnits="1", memorySize = "2.0f")
    Integer qctoolS(
        @Parameter(type = Type.STRING, direction = Direction.IN) String gPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String imputeFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String ogPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String filteredFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String inclRSIdsPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.IN) String inclusionRsIdFile,
        @Parameter(type = Type.STRING, direction = Direction.IN) String omitChromosomeFlag, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String forceFlag, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String logPrefix, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String filteredLogFile, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String mafPrefix, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String mafThresholdS, 
        @Parameter(type = Type.STRING, direction = Direction.IN) String oneFlag, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDOUT) String stdOutFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT, stream = Stream.STDERR) String stdErrorFile
    );
    
    @Method(declaringClass = "guidance.GuidanceImpl")
    @Constraints(computingUnits="1", memorySize = "2.0f")
    void postQCTool(
        @Parameter(type = Type.FILE, direction = Direction.IN) String filteredFile, 
        @Parameter(type = Type.FILE, direction = Direction.OUT) String filteredFileGz
    );
    
    @Binary(binary = "$" + Environment.EV_SHAPEITBINARY)
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
    @Constraints(computingUnits="1", memorySize = "1.0f")
    void createRsIdList(
       @Parameter(type = Type.FILE,   direction = Direction.IN)  String genFile,
       @Parameter(type = Type.STRING, direction = Direction.IN)  String exclCgatFlag,
       @Parameter(type = Type.FILE,   direction = Direction.OUT) String pairsFile,
       @Parameter(type = Type.STRING, direction = Direction.IN)  String inputFormat
    );

    @Method(declaringClass = "guidance.GuidanceImpl")
    @Constraints(computingUnits="1", memorySize = "4.0f")
    void createListOfExcludedSnps(
       @Parameter(type = Type.FILE, direction = Direction.IN) String shapeitHapsFile,
       @Parameter(type = Type.FILE, direction = Direction.OUT) String excludedSnpsFile,
       @Parameter(type = Type.STRING, direction = Direction.IN) String exclCgatFlag,
       @Parameter(type = Type.STRING, direction = Direction.IN) String exclSVFlag
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
       @Parameter(type = Type.STRING, direction = Direction.IN) String mafControlsThreshold
    );

    @Method(declaringClass = "guidance.GuidanceImpl")
    @Constraints(computingUnits="1", memorySize = "6.0f")
    void filterByInfo(
       @Parameter(type = Type.FILE, direction = Direction.IN) String imputeFileInfo,
       @Parameter(type = Type.FILE, direction = Direction.OUT) String filteredFile,
       @Parameter(type = Type.STRING, direction = Direction.IN) String threshold
    );

    @Method(declaringClass = "guidance.GuidanceImpl")
    @Constraints(computingUnits="1", memorySize = "6.0f")
    void jointFilteredByAllFiles(
       @Parameter(type = Type.FILE, direction = Direction.IN) String filteredByAllA,
       @Parameter(type = Type.FILE, direction = Direction.IN) String filteredByAllB,
       @Parameter(type = Type.FILE, direction = Direction.OUT) String filteredByAllC,
       @Parameter(type = Type.STRING, direction = Direction.IN) String rpanelName,
       @Parameter(type = Type.STRING, direction = Direction.IN) String rpanelFlag
    );

    @Method(declaringClass = "guidance.GuidanceImpl")
    @Constraints(computingUnits="1", memorySize = "6.0f")
    void jointCondensedFiles(
       @Parameter(type = Type.FILE, direction = Direction.IN) String inputAFile,
       @Parameter(type = Type.FILE, direction = Direction.IN) String inputBFile,
       @Parameter(type = Type.FILE, direction = Direction.OUT) String outputFile
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
       @Parameter(type = Type.STRING, direction = Direction.IN) String hweControlsThresholdS
    );

    @Method(declaringClass = "guidance.GuidanceImpl")
    @Constraints(computingUnits="1", memorySize = "8.0f")
    void initPhenoMatrix(
       @Parameter(type = Type.FILE, direction = Direction.IN) String topHitsFile,
       @Parameter(type = Type.STRING, direction = Direction.IN) String ttName,
       @Parameter(type = Type.STRING, direction = Direction.IN) String rpName,
       @Parameter(type = Type.FILE, direction = Direction.OUT) String phenomeFile
    );

    @Method(declaringClass = "guidance.GuidanceImpl")
    @Constraints(computingUnits="1", memorySize = "8.0f")
    void addToPhenoMatrix(
       @Parameter(type = Type.FILE, direction = Direction.IN) String phenomeAFile,
       @Parameter(type = Type.FILE, direction = Direction.IN) String topHitsFile,
       @Parameter(type = Type.STRING, direction = Direction.IN) String ttName,
       @Parameter(type = Type.STRING, direction = Direction.IN) String rpName,
       @Parameter(type = Type.FILE, direction = Direction.OUT) String phenomeBFile
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
       @Parameter(type = Type.FILE, direction = Direction.OUT) String phenomeBFile
    );

    @Method(declaringClass = "guidance.GuidanceImpl")
    @Constraints(computingUnits="1", memorySize = "8.0f")
    void finalizePhenoMatrix(
       @Parameter(type = Type.FILE, direction = Direction.IN) String phenomeAFile,
       @Parameter(type = Type.FILE, direction = Direction.IN) String phenomeBFile,
       @Parameter(type = Type.STRING, direction = Direction.IN) String ttName,
       @Parameter(type = Type.STRING, direction = Direction.IN) String rpName,
       @Parameter(type = Type.FILE, direction = Direction.OUT) String phenomeCFile
    );

    @Method(declaringClass = "guidance.GuidanceImpl")
    @Constraints(computingUnits="1", memorySize = "8.0f")
    void mergeTwoChunks(
       @Parameter(type = Type.FILE, direction = Direction.IN) String reduceFileA,
       @Parameter(type = Type.FILE, direction = Direction.IN) String reduceFileB,
       @Parameter(type = Type.FILE, direction = Direction.OUT) String reduceFileC,
       @Parameter(type = Type.STRING, direction = Direction.IN) String chrS
    );

    @Method(declaringClass = "guidance.GuidanceImpl")
    @Constraints(computingUnits="1", memorySize = "20.0f")
    void combinePanelsComplex(
       @Parameter(type = Type.FILE, direction = Direction.IN) String resultsFileA,
       @Parameter(type = Type.FILE, direction = Direction.IN) String resultsFileB,
       @Parameter(type = Type.FILE, direction = Direction.OUT) String resultsFileC,
       @Parameter(type = Type.STRING, direction = Direction.IN) String chromoStart,
       @Parameter(type = Type.STRING, direction = Direction.IN) String chromoEnd
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
       @Parameter(type = Type.STRING, direction = Direction.IN) String mafControlsThreshold
    );

    @Method(declaringClass = "guidance.GuidanceImpl")
    @Constraints(computingUnits="1", memorySize = "8.0f")
    void generateTopHitsAll(
       @Parameter(type = Type.FILE, direction = Direction.IN) String resultsAFile,
       @Parameter(type = Type.FILE, direction = Direction.IN) String resultsBFile,
       @Parameter(type = Type.FILE, direction = Direction.OUT) String outputTopHitsFile,
       @Parameter(type = Type.STRING, direction = Direction.IN) String pvaThreshold
    );
    
}
