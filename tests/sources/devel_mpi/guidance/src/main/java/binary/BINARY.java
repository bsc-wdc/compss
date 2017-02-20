package binary;

public class BINARY {

    /**
     * Method to perform the BED phasing
     * 
     * @param inputBedPrefix
     * @param bedFile
     * @param bimFile
     * @param famFile
     * @param inputMapPrefix
     * @param gmapFile
     * @param chrXPrefix
     * @param outputMaxPrefix
     * @param shapeitHapsFile
     * @param shapeitSampleFile
     * @param threadPrefix
     * @param numThreads
     * @param effectiveSizePrefix
     * @param effectiveSize
     * @param outputLogPrefix
     * @param shapeitLogFile
     * @param shapeitStdOut
     * @param shapeitStdErr
     * @return
     */
    public static Integer phasingBed(String inputBedPrefix, String bedFile, String bimFile, String famFile, String inputMapPrefix,
            String gmapFile, String chrXPrefix, String outputMaxPrefix, String shapeitHapsFile, String shapeitSampleFile,
            String threadPrefix, int numThreads, String effectiveSizePrefix, int effectiveSize, String outputLogPrefix,
            String shapeitLogFile, String shapeitStdOut, String shapeitStdErr) {

        return null;
    }

    /**
     * Method to execute filterHaplotypes where input files are in GEN format
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
     * @param chrPrefix
     * @param chromo
     * @param recodeFlag
     * @param outPrefix
     * @param newBedFileName
     * @param makeBedFlag
     * @param newBedFile
     * @param newBimFile
     * @param newFamFile
     * @param newLogFile
     * @return
     */
    public static Integer convertFromBedToBed(String noWebFlag, String bedPrefix, String bedFile, String bimPrefix, String bimFile,
            String famPrefix, String famFile, String chrPrefix, String chromo, String recodeFlag, String outPrefix, String newBedFileName,
            String makeBedFlag, String newBedFile, String newBimFile, String newFamFile, String newLogFile, String stdout, String stderr) {

        return null;
    }

}
