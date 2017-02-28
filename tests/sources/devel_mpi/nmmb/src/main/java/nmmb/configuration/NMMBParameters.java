package nmmb.configuration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import nmmb.exceptions.CommandException;
import nmmb.loggers.LoggerNames;
import nmmb.utils.BashCMDExecutor;
import nmmb.utils.FileManagement;


public class NMMBParameters {

    // Loggers
    private static final Logger LOGGER_MAIN = LogManager.getLogger(LoggerNames.NMMB_MAIN);
    private static final Logger LOGGER_FIXED = LogManager.getLogger(LoggerNames.NMMB_FIXED);

    // -----------------------------------------------------------------------
    // MN settings
    public int INPES;
    public int JNPES;
    public int WRTSK;
    public int PROC;

    // -----------------------------------------------------------------------
    // Global-regional switch - Model domain setup global/regional
    public boolean DOMAIN;
    public int LM;
    public String CASE;

    // -----------------------------------------------------------------------
    // Model variables
    public int DT_INT;
    public double TLM0D;
    public double TPH0D;
    public double WBD;
    public double SBD;
    public double DLMD;
    public double DPHD;
    public double PTOP;
    public double DCAL;
    public int NRADS;
    public int NRADL;
    public int IMI;
    public int JMI;
    public int IM;
    public int JM;

    // -----------------------------------------------------------------------
    // Case selection
    public boolean DO_FIXED;
    public boolean DO_VRBL;
    public boolean DO_UMO;
    public boolean DO_POST;

    // -----------------------------------------------------------------------
    // Select START and ENDING Times
    public Date START_DATE;
    public Date END_DATE;

    // -----------------------------------------------------------------------
    // Select configuration of POSTPROC (DO_POST)

    // -----------------------------------------------------------------------
    // Select IC of chemistry for run with COUPLE_DUST_INIT=0
    public int INIT_CHEM;


    // -----------------------------------------------------------------------
    // Couple dust

    /**
     * Constructor
     * 
     * @param nmmbConfiguration
     */
    public NMMBParameters(NMMBConfigManager nmmbConfiguration) {
        LOGGER_MAIN.info("Setting execution variables...");

        // MN settings
        INPES = nmmbConfiguration.getINPES();
        JNPES = nmmbConfiguration.getJNPES();
        WRTSK = nmmbConfiguration.getWRTSK();
        PROC = INPES * JNPES + WRTSK;

        // Global-regional switch - Model domain setup global/regional
        DOMAIN = nmmbConfiguration.getDomain();
        LM = nmmbConfiguration.getLM();
        CASE = nmmbConfiguration.getCase();

        // Model variables
        DT_INT = (DOMAIN) ? nmmbConfiguration.getDT_INT1() : nmmbConfiguration.getDT_INT2();
        TLM0D = (DOMAIN) ? nmmbConfiguration.getTLM0D1() : nmmbConfiguration.getTLM0D2();
        TPH0D = (DOMAIN) ? nmmbConfiguration.getTPH0D1() : nmmbConfiguration.getTPH0D2();
        WBD = (DOMAIN) ? nmmbConfiguration.getWBD1() : nmmbConfiguration.getWBD2();
        SBD = (DOMAIN) ? nmmbConfiguration.getSBD1() : nmmbConfiguration.getSBD2();
        DLMD = (DOMAIN) ? nmmbConfiguration.getDLMD1() : nmmbConfiguration.getDLMD2();
        DPHD = (DOMAIN) ? nmmbConfiguration.getDPHD1() : nmmbConfiguration.getDPHD2();
        PTOP = (DOMAIN) ? nmmbConfiguration.getPTOP1() : nmmbConfiguration.getPTOP2();
        DCAL = (DOMAIN) ? nmmbConfiguration.getDCAL1() : nmmbConfiguration.getDCAL2();
        NRADS = (DOMAIN) ? nmmbConfiguration.getNRADS1() : nmmbConfiguration.getNRADS2();
        NRADL = (DOMAIN) ? nmmbConfiguration.getNRADL1() : nmmbConfiguration.getNRADL2();
        IMI = (int) (-2.0 * WBD / DLMD + 1.5);
        JMI = (int) (-2.0 * SBD / DPHD + 1.5);
        IM = (DOMAIN) ? IMI + 2 : IMI;
        JM = (DOMAIN) ? JMI + 2 : JMI;

        LOGGER_MAIN.info("");
        LOGGER_MAIN.info("Number of processors " + PROC);
        LOGGER_MAIN.info("Model grid size - IM / JM / LM: " + IMI + " / " + JMI + " / " + LM);
        LOGGER_MAIN.info("Extended domain - IM / JM / LM: " + IM + " / " + JM + " / " + LM);
        LOGGER_MAIN.info("");

        // Case selection
        DO_FIXED = nmmbConfiguration.getFixed();
        DO_VRBL = nmmbConfiguration.getVariable();
        DO_UMO = nmmbConfiguration.getUmoModel();
        DO_POST = nmmbConfiguration.getPost();

        // -----------------------------------------------------------------------
        // Select START and ENDING Times
        try {
            START_DATE = NMMBConstants.STR_TO_DATE.parse(nmmbConfiguration.getStartDate());
        } catch (ParseException pe) {
            LOGGER_MAIN.error("[ERROR] Cannot parse start date", pe);
            LOGGER_MAIN.error("Aborting...");
            System.exit(1);
        }
        try {
            END_DATE = NMMBConstants.STR_TO_DATE.parse(nmmbConfiguration.getEndDate());
        } catch (ParseException pe) {
            LOGGER_MAIN.error("[ERROR] Cannot parse end date", pe);
            LOGGER_MAIN.error("Aborting...");
            System.exit(1);
        }

        // -----------------------------------------------------------------------
        // Select configuration of POSTPROC (DO_POST)

        // -----------------------------------------------------------------------
        // Select IC of chemistry for run with COUPLE_DUST_INIT=0
        INIT_CHEM = nmmbConfiguration.getInitChem();

        LOGGER_MAIN.info("Execution variables set");
    }

    public void prepareExecution() {
        LOGGER_MAIN.info("Preparing execution...");

        // Define folders
        String outputPath = NMMBEnvironment.UMO_OUT;
        String outputCasePath = NMMBEnvironment.OUTNMMB + CASE + File.separator + "output" + File.separator;
        String outputSymPath = NMMBEnvironment.UMO_PATH + "PREPROC" + File.separator + "output";

        // Clean folders
        LOGGER_MAIN.debug("Clean output folder : " + outputPath);
        FileManagement.deleteFileOrFolder(new File(outputPath));
        LOGGER_MAIN.debug("Clean output folder : " + outputCasePath);
        FileManagement.deleteFileOrFolder(new File(outputCasePath));
        LOGGER_MAIN.debug("Clean output folder : " + outputSymPath);
        FileManagement.deleteFileOrFolder(new File(outputSymPath));

        // Create empty files
        LOGGER_MAIN.debug("Create output folder : " + outputPath);
        if (!new File(outputPath).mkdirs()) {
            LOGGER_MAIN.error("[ERROR] Cannot create output folder");
            LOGGER_MAIN.error("Aborting...");
            System.exit(1);
        }
        LOGGER_MAIN.debug("Create output folder : " + outputCasePath);
        if (!new File(outputCasePath).mkdirs()) {
            LOGGER_MAIN.error("[ERROR] Cannot create output case folder");
            LOGGER_MAIN.error("Aborting...");
            System.exit(1);
        }

        // Symbolic link for preprocess
        LOGGER_MAIN.debug("Symbolic link for PREPROC output folder");
        LOGGER_MAIN.debug("   - From: " + outputCasePath);
        LOGGER_MAIN.debug("   - To:   " + outputSymPath);
        try {
            Files.createSymbolicLink(Paths.get(outputSymPath), Paths.get(outputCasePath));
        } catch (UnsupportedOperationException | IOException | SecurityException | InvalidPathException exception) {
            LOGGER_MAIN.error("[ERROR] Cannot create output symlink", exception);
            LOGGER_MAIN.error("Aborting...");
            System.exit(1);
        }

        LOGGER_MAIN.info("Execution environment prepared");
    }

    public void prepareFixedExecution() {
        LOGGER_FIXED.debug("   - INCLUDE PATH : " + NMMBEnvironment.INCLUDE_DIR);

        String modelgridTMPFilePath = NMMBEnvironment.INCLUDE_DIR + "modelgrid_rrtm.tmp";
        String lmimjmTMPFilePath = NMMBEnvironment.INCLUDE_DIR + "lmimjm_rrtm.tmp";
        String modelgridFilePath = NMMBEnvironment.INCLUDE_DIR + "modelgrid.inc";
        String lmimjmFilePath = NMMBEnvironment.INCLUDE_DIR + "lmimjm.inc";

        // Clean some files
        LOGGER_FIXED.debug("Delete previous: " + modelgridFilePath);
        if (!FileManagement.deleteFile(modelgridFilePath)) {
            LOGGER_FIXED.debug("Cannot erase previous modelgrid because it doesn't exist.");
        }
        LOGGER_FIXED.debug("Delete previous: " + lmimjmFilePath);
        if (!FileManagement.deleteFile(lmimjmFilePath)) {
            LOGGER_FIXED.debug("Cannot erase previous lmimjm because it doesn't exist.");
        }

        // Prepare files
        BashCMDExecutor cmdModelgrid = new BashCMDExecutor("sed");
        cmdModelgrid.addFlagAndValue("-e", "s/TLMD/" + String.valueOf(TLM0D) + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/TPHD/" + String.valueOf(TPH0D) + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/WBDN/" + String.valueOf(WBD) + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/SBDN/" + String.valueOf(SBD) + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/DLMN/" + String.valueOf(DLMD) + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/DPHN/" + String.valueOf(DPHD) + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/III/" + String.valueOf(IMI) + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/JJJ/" + String.valueOf(JMI) + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/IBDY/" + String.valueOf(IM) + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/JBDY/" + String.valueOf(JM) + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/PTOP/" + String.valueOf(PTOP) + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/KKK/" + String.valueOf(LM) + "/");
        cmdModelgrid.addArgument(modelgridTMPFilePath);
        cmdModelgrid.redirectOutput(modelgridFilePath);
        try {
            int ev = cmdModelgrid.execute();
            if (ev != 0) {
                throw new CommandException("[ERROR] CMD returned non-zero exit value: " + ev);
            }
        } catch (CommandException ce) {
            LOGGER_FIXED.error("[ERROR] Error performing sed command on model grid " + modelgridTMPFilePath, ce);
            LOGGER_FIXED.error("Aborting...");
            System.exit(1);
        }

        BashCMDExecutor cmdLmimjm = new BashCMDExecutor("sed");
        cmdLmimjm.addFlagAndValue("-e", "s/TLMD/" + String.valueOf(TLM0D) + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/TPHD/" + String.valueOf(TPH0D) + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/WBDN/" + String.valueOf(WBD) + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/SBDN/" + String.valueOf(SBD) + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/DLMN/" + String.valueOf(DLMD) + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/DPHN/" + String.valueOf(DPHD) + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/III/" + String.valueOf(IMI) + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/JJJ/" + String.valueOf(JMI) + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/IBDY/" + String.valueOf(IM) + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/JBDY/" + String.valueOf(JM) + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/PTOP/" + String.valueOf(PTOP) + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/KKK/" + String.valueOf(LM) + "/");
        cmdLmimjm.addArgument(lmimjmTMPFilePath);
        cmdLmimjm.redirectOutput(lmimjmFilePath);
        try {
            int ev = cmdLmimjm.execute();
            if (ev != 0) {
                throw new CommandException("[ERROR] CMD returned non-zero exit value: " + ev);
            }
        } catch (CommandException ce) {
            LOGGER_FIXED.error("[ERROR] Error performing sed command on Lmimjm " + lmimjmTMPFilePath, ce);
            LOGGER_FIXED.error("Aborting...");
            System.exit(1);
        }
    }

}
