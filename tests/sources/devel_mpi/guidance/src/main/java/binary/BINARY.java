package binary;

public class BINARY {

    /**
     * Method to perform the conversion from Bed to Ped Format file
     * 
     * @param noWebFlag
     * @param bedPrefix
     * @param bedFile
     * @param bimPrefix
     * @param bimFile
     * @param famPrefix
     * @param famFile
     * @param chromoPrefix
     * @param chromo
     * @param recodeFlag
     * @param outPrefix
     * @param newBedFile
     * @param makeBedFlag
     * @param newBimFile
     * @param newFamFile
     * @param newLogFile
     * @param stdOutputFile
     * @param stdErrorFile
     * @return
     */
    public static Integer convertFromBedToBed(String noWebFlag, String bedPrefix, String bedFile, String bimPrefix, String bimFile,
            String famPrefix, String famFile, String chromoPrefix, String chromo, String recodeFlag, String outPrefix, String newBedFile,
            String makeBedFlag, String newBimFile, String newFamFile, String newLogFile, String stdOutputFile, String stdErrorFile) {

        return null;
    }

    /**
     * Method to perform the BED phasing
     * 
     * @param inputBedPrefix
     * @param bedFile
     * @param bumFile
     * @param famFile
     * @param inputMapPrefix
     * @param gmapFile
     * @param chromoFlag
     * @param outputPrefix
     * @param shapeitHapsFile
     * @param shapeitSampleFile
     * @param threadPrefix
     * @param numThreads
     * @param effectiveSizePrefix
     * @param effectiveSize
     * @param outputLogPrefix
     * @param shapeitLogFile
     * @param stdOutputFile
     * @param stdErrorFile
     * @return
     */
    public static Integer phasingBed(String inputBedPrefix, String bedFile, String bumFile, String famFile, String inputMapPrefix,
            String gmapFile, String chromoFlag, String outputPrefix, String shapeitHapsFile, String shapeitSampleFile, String threadPrefix,
            int numThreads, String effectiveSizePrefix, int effectiveSize, String outputLogPrefix, String shapeitLogFile,
            String stdOutputFile, String stdErrorFile) {

        return null;
    }

    /**
     * Method to do phasing
     * 
     * @param inputGenPrefix
     * @param inputGenFile
     * @param inputSampleFile
     * @param inputMapPrefix
     * @param gmapFile
     * @param chromoFlag
     * @param outputPrefix
     * @param shapeitHapsFile
     * @param shapeitSampleFile
     * @param threadPrefix
     * @param numThreads
     * @param effectiveSizePrefix
     * @param effectiveSize
     * @param outputLogPrefix
     * @param shapeitLogFile
     * @param stdOutFile
     * @param stdErrorFile
     * @return
     */
    public static Integer phasing(String inputGenPrefix, String inputGenFile, String inputSampleFile, String inputMapPrefix,
            String gmapFile, String chromoFlag, String outputPrefix, String shapeitHapsFile, String shapeitSampleFile, String threadPrefix,
            int numThreads, String effectiveSizePrefix, int effectiveSize, String outputLogPrefix, String shapeitLogFile, String stdOutFile,
            String stdErrorFile) {

        return null;
    }

    /**
     * Impute with impute
     * 
     * @param prephasedFlag
     * @param mPrefix
     * @param gmapFile
     * @param hPrefix
     * @param knownHapFile
     * @param lPrefix
     * @param legendFile
     * @param knownHapsPrefix
     * @param shapeitHapsFile
     * @param samplePrefix
     * @param shapeitSampleFile
     * @param intPrefix
     * @param lim1S
     * @param lim2S
     * @param chromoFlag
     * @param exludeSnpsPrefix
     * @param pairsFile
     * @param imputeExcludedFlag
     * @param nePrefix
     * @param ne
     * @param oPrefix
     * @param imputeFile
     * @param iPrefix
     * @param imputeFileInfo
     * @param rPrefix
     * @param imputeFileSummary
     * @param wPrefix
     * @param imputeFileWarnings
     * @param noSampleQCFlag
     * @param oGzFlag
     * @param stdOutFile
     * @param stdErrorFile
     * @return
     */
    public static Integer imputeWithImpute(String prephasedFlag, String mPrefix, String gmapFile, String hPrefix, String knownHapFile,
            String lPrefix, String legendFile, String knownHapsPrefix, String shapeitHapsFile, String samplePrefix,
            String shapeitSampleFile, String intPrefix, String lim1S, String lim2S, String chromoFlag, String exludeSnpsPrefix,
            String pairsFile, String imputeExcludedFlag, String nePrefix, int ne, String oPrefix, String imputeFile, String iPrefix,
            String imputeFileInfo, String rPrefix, String imputeFileSummary, String wPrefix, String imputeFileWarnings,
            String noSampleQCFlag, String oGzFlag, String stdOutFile, String stdErrorFile) {

        return null;
    }

    /**
     * Impute with Minimac
     * 
     * @param vcfReferenceFlag
     * @param refHapsPrefix
     * @param knownHapFile
     * @param snpsPrefix
     * @param filteredListOfSnpsFile
     * @param shapeHapsPrefix
     * @param filteredHapsFile
     * @param samplePrefix
     * @param filteredSampleFile
     * @param vcfStartPrefix
     * @param lim1S
     * @param vcfEndPrefix
     * @param lim2S
     * @param chromoFlag
     * @param chromo
     * @param vcfWindowPrefix
     * @param vcfWindow
     * @param roundsPrefix
     * @param rounds
     * @param statesPrefix
     * @param states
     * @param prefixPrefix
     * @param imputedMMFileName
     * @param gzipFlag
     * @param imputedMMInfoFile
     * @param imputedMMErateFile
     * @param imputedMMRecFile
     * @param imputedMMDoseFile
     * @param stdOutFile
     * @param stdErrFile
     * @return
     */
    public static Integer imputeWithMinimac(String vcfReferenceFlag, String refHapsPrefix, String knownHapFile, String snpsPrefix,
            String filteredListOfSnpsFile, String shapeHapsPrefix, String filteredHapsFile, String samplePrefix, String filteredSampleFile,
            String vcfStartPrefix, String lim1S, String vcfEndPrefix, String lim2S, String chromoFlag, String chromo,
            String vcfWindowPrefix, int vcfWindow, String roundsPrefix, int rounds, String statesPrefix, int states, String prefixPrefix,
            String imputedMMFileName, String gzipFlag, String imputedMMInfoFile, String imputedMMErateFile, String imputedMMRecFile,
            String imputedMMDoseFile, String stdOutFile, String stdErrFile) {

        return null;
    }

    /**
     * Generate QQ Manhattan
     * 
     * @param inputFile
     * @param qqPlotFile
     * @param manhattanPlotFile
     * @param qqPlotTiffFile
     * @param manhattanPlotTiffFile
     * @param correctedPvaluesFile
     * @param stdOutFile
     * @param stdErrorFile
     * @return
     */
    public static Integer generateQQManhattanPlots(String inputFile, String qqPlotFile, String manhattanPlotFile, String qqPlotTiffFile,
            String manhattanPlotTiffFile, String correctedPvaluesFile, String stdOutFile, String stdErrorFile) {

        return null;
    }

    /**
     * SNP Test binary
     * 
     * @param dataPrefix
     * @param mergedGenFile
     * @param mergedSampleFile
     * @param oPrefix
     * @param snpTestOutFile
     * @param phenoPrefix
     * @param responseVar
     * @param covarsFlag
     * @param hweFlag
     * @param logPrefix
     * @param snpTestLogFile
     * @param chromoFlag
     * @param stdOutFile
     * @param stdErrorFile
     * @return
     */
    public static Integer snptest(String dataPrefix, String mergedGenFile, String mergedSampleFile, String oPrefix, String snpTestOutFile,
            String phenoPrefix, String responseVar, String covarsFlag, String hweFlag, String logPrefix, String snpTestLogFile,
            String chromoFlag, String stdOutFile, String stdErrorFile) {

        return null;
    }

    /**
     * QCTool binary
     * 
     * @param gPrefix
     * @param imputeFile
     * @param ogPrefix
     * @param filteredFile
     * @param inclRSIdsPrefix
     * @param inclusionRsIdFile
     * @param omitChromosomeFlag
     * @param forceFlag
     * @param logPrefix
     * @param filteredLogFile
     * @param mafPrefix
     * @param mafThresholdS
     * @param oneFlag
     * @param stdOutFile
     * @param stdErrorFile
     * @return
     */
    public static Integer qctoolS(String gPrefix, String imputeFile, String ogPrefix, String filteredFile, String inclRSIdsPrefix,
            String inclusionRsIdFile, String omitChromosomeFlag, String forceFlag, String logPrefix, String filteredLogFile,
            String mafPrefix, String mafThresholdS, String oneFlag, String stdOutFile, String stdErrorFile) {

        return null;
    }

    /**
     * Filter Haplotypes
     * 
     * @param convertFlag
     * @param inputHapsPrefix
     * @param hapsFile
     * @param sampleFile
     * @param exludeSnpPrefix
     * @param excludedSnpsFile
     * @param outputHapsPrefix
     * @param filteredHapsFile
     * @param filteredSampleFile
     * @param outputLogPrefix
     * @param filteredLogFile
     * @param outputVcfPrefix
     * @param filteredHapsVcfFile
     * @return
     */
    public static Integer filterHaplotypes(String convertFlag, String inputHapsPrefix, String hapsFile, String sampleFile,
            String exludeSnpPrefix, String excludedSnpsFile, String outputHapsPrefix, String filteredHapsFile, String filteredSampleFile,
            String outputLogPrefix, String filteredLogFile, String outputVcfPrefix, String filteredHapsVcfFile) {

        return null;
    }

}
