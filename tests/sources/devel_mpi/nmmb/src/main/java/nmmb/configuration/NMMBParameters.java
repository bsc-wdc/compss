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


/**
 * Representation of the parameters of a NMMB execution
 * 
 */
public class NMMBParameters {

    // Loggers
    private static final Logger LOGGER_MAIN = LogManager.getLogger(LoggerNames.NMMB_MAIN);
    private static final Logger LOGGER_FIXED = LogManager.getLogger(LoggerNames.NMMB_FIXED);
    private static final Logger LOGGER_VARIABLE = LogManager.getLogger(LoggerNames.NMMB_VARIABLE);
    private static final Logger LOGGER_UMO_MODEL = LogManager.getLogger(LoggerNames.NMMB_UMO_MODEL);
    private static final Logger LOGGER_POST = LogManager.getLogger(LoggerNames.NMMB_POST);

    // -----------------------------------------------------------------------
    // MN settings
    private int INPES;
    private int JNPES;
    private int WRTSK;
    private int PROC;

    // -----------------------------------------------------------------------
    // Global-regional switch - Model domain setup global/regional
    private boolean DOMAIN;
    private int LM;
    private String CASE;

    // -----------------------------------------------------------------------
    // Model variables
    private int DT_INT;
    private double TLM0D;
    private double TPH0D;
    private double WBD;
    private double SBD;
    private double DLMD;
    private double DPHD;
    private double PTOP;
    private double DCAL;
    private int NRADS;
    private int NRADL;
    private int IMI;
    private int JMI;
    private int IM;
    private int JM;

    // -----------------------------------------------------------------------
    // Case selection
    private boolean DO_FIXED;
    private boolean DO_VRBL;
    private boolean DO_UMO;
    private boolean DO_POST;

    // -----------------------------------------------------------------------
    // Select START and ENDING Times
    private Date START_DATE;
    private Date END_DATE;
    private int HOUR;
    private int NHOURS;
    private int NHOURS_INIT;
    private int HIST;
    private int HIST_M;
    private int BOCO;
    private String TYPE_GFSINIT;

    // -----------------------------------------------------------------------
    // Select configuration of POSTPROC (DO_POST)
    private int HOUR_P;
    private int NHOURS_P;
    private int HIST_P;
    private int LSM;
    private double TPH0DN;
    private double WBDDEF;
    private double SBDDEF;

    // -----------------------------------------------------------------------
    // Select IC of chemistry for run with COUPLE_DUST_INIT=0
    private int INIT_CHEM;

    // -----------------------------------------------------------------------
    // Couple dust
    private boolean COUPLE_DUST;
    private boolean COUPLE_DUST_INIT;


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
        HOUR = nmmbConfiguration.getHour();
        NHOURS = nmmbConfiguration.getNHours();
        NHOURS_INIT = nmmbConfiguration.getNHoursInit();
        HIST = nmmbConfiguration.getHist();
        HIST_M = HIST * NMMBConstants.HOUR_TO_MINUTES;
        BOCO = nmmbConfiguration.getBoco();
        TYPE_GFSINIT = nmmbConfiguration.getTypeGFSInit();

        // -----------------------------------------------------------------------
        // Select configuration of POSTPROC (DO_POST)
        HOUR_P = nmmbConfiguration.getHourP();
        ;
        NHOURS_P = nmmbConfiguration.getNHoursP();
        HIST_P = nmmbConfiguration.getHistP();
        LSM = nmmbConfiguration.getLSM();

        TPH0DN = nmmbConfiguration.getTPH0D2() + 90.0;
        WBDDEF = nmmbConfiguration.getWBD2() + nmmbConfiguration.getTLM0D2();
        SBDDEF = nmmbConfiguration.getSBD2() + nmmbConfiguration.getTPH0D2();

        // -----------------------------------------------------------------------
        // Select IC of chemistry for run with COUPLE_DUST_INIT=0
        INIT_CHEM = nmmbConfiguration.getInitChem();
        COUPLE_DUST = nmmbConfiguration.getCoupleDust();
        COUPLE_DUST_INIT = nmmbConfiguration.getCoupleDustInit();

        LOGGER_MAIN.info("Execution variables set");
    }

    /**
     * Returns the DOMAIN value
     * 
     * @return
     */
    public boolean getDomain() {
        return this.DOMAIN;
    }

    /**
     * Returns the CASE value
     * 
     * @return
     */
    public String getCase() {
        return this.CASE;
    }

    /**
     * Returns the DO_FIXED value
     * 
     * @return
     */
    public boolean doFixed() {
        return this.DO_FIXED;
    }

    /**
     * Returns the DO_VARIABLE value
     * 
     * @return
     */
    public boolean doVariable() {
        return this.DO_VRBL;
    }

    /**
     * Returns the DO_UMO value
     * 
     * @return
     */
    public boolean doUmoModel() {
        return this.DO_UMO;
    }

    /**
     * Returns the DO_POST value
     * 
     * @return
     */
    public boolean doPost() {
        return this.DO_POST;
    }

    /**
     * Returns the START_DATE
     * 
     * @return
     */
    public Date getStartDate() {
        return this.START_DATE;
    }

    /**
     * Returns the END_DATE
     * 
     * @return
     */
    public Date getEndDate() {
        return this.END_DATE;
    }

    /**
     * Returns the HOUR value
     * 
     * @return
     */
    public int getHour() {
        return this.HOUR;
    }

    /**
     * Returns the COUPLE_DUST_INIT value
     * 
     * @return
     */
    public boolean getCoupleDustInit() {
        return this.COUPLE_DUST_INIT;
    }

    /**
     * Actions to perform to setup an NMMB execution
     * 
     */
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
        if (!FileManagement.createDir(outputPath)) {
            LOGGER_MAIN.error("[ERROR] Cannot create output folder");
            LOGGER_MAIN.error("Aborting...");
            System.exit(1);
        }
        LOGGER_MAIN.debug("Create output folder : " + outputCasePath);
        if (!FileManagement.createDir(outputCasePath)) {
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

    /*
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ******************** FIXED STEP *******************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     */

    /**
     * Actions to setup the FIXED Step of an NMMB execution
     * 
     */
    public void prepareFixedExecution() {
        LOGGER_FIXED.debug("   - INCLUDE PATH : " + NMMBEnvironment.FIX_INCLUDE_DIR);

        String modelgridTMPFilePath = NMMBEnvironment.FIX_INCLUDE_DIR + "modelgrid_rrtm.tmp";
        String lmimjmTMPFilePath = NMMBEnvironment.FIX_INCLUDE_DIR + "lmimjm_rrtm.tmp";
        String modelgridFilePath = NMMBEnvironment.FIX_INCLUDE_DIR + "modelgrid.inc";
        String lmimjmFilePath = NMMBEnvironment.FIX_INCLUDE_DIR + "lmimjm.inc";

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

    /*
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ******************** OUTPUT FOLDERS ***************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     */

    /**
     * Actions to create the output folder for a date iteration of an NMMB execution
     * 
     * @param currentDate
     */
    public void createOutputFolders(Date currentDate) {
        String currentDateSTR = NMMBConstants.STR_TO_DATE.format(currentDate);
        String hourSTR = (HOUR < 10) ? "0" + String.valueOf(HOUR) : String.valueOf(HOUR);
        String folderOutputCase = NMMBEnvironment.OUTNMMB + CASE + File.separator;
        String folderOutput = NMMBEnvironment.OUTNMMB + CASE + File.separator + currentDateSTR + hourSTR + File.separator;

        if (!FileManagement.createDir(folderOutputCase)) {
            LOGGER_MAIN.debug("Cannot create folder output case : " + folderOutputCase + " because it already exists. Skipping");
        }

        if (!FileManagement.createDir(folderOutput)) {
            LOGGER_MAIN.debug("Cannot create folder output : " + folderOutput + " because it already exists. Skipping");
        }
    }

    /*
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ******************** VARIABLE STEP ****************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     */

    /**
     * Actions to prepare the VARIABLE Step of an NMMB Execution
     * 
     * @param currentDate
     */
    public void prepareVariableExecution(Date currentDate) {
        // Clean specific files
        final String[] outputFiles = new String[] { "sst2dvar_grb_0.5", "fcst", "llstmp", "llsmst", "llgsno", "llgcic", "llgsst",
                "llspl.000", "llgsst05", "albedo", "albase", "vegfrac", "z0base", "z0", "ustar", "sst05", "dzsoil", "tskin", "sst", "snow",
                "snowheight", "cice", "seamaskcorr", "landusecorr", "landusenewcorr", "topsoiltypecorr", "vegfraccorr", "z0corr",
                "z0basecorr", "emissivity", "canopywater", "frozenprecratio", "smst", "sh2o", "stmp" };

        for (String file : outputFiles) {
            String filePath = NMMBEnvironment.OUTPUT + file;
            if (!FileManagement.deleteFile(filePath)) {
                LOGGER_VARIABLE.debug("Cannot erase previous " + file + " because it doesn't exist.");
            }
        }

        // Clean regular expr files
        File folder = new File(NMMBEnvironment.OUTPUT);
        for (File file : folder.listFiles()) {
            if ((file.getName().endsWith(".gfs")) || (file.getName().startsWith("gfs.")) || (file.getName().startsWith("boco."))
                    || (file.getName().startsWith("boco_chem."))) {

                if (!FileManagement.deleteFile(file)) {
                    LOGGER_VARIABLE.debug("Cannot erase previous " + file.getName() + " because it doesn't exist.");
                }

            }
        }

        // Clean files on VRB
        String sstgrbFilePath = NMMBEnvironment.VRB + "sstgrb";
        if (!FileManagement.deleteFile(sstgrbFilePath)) {
            LOGGER_VARIABLE.debug("Cannot erase previous sstgrb because it doesn't exist.");
        }

        String llgridFilePath = NMMBEnvironment.VRB_INCLUDE_DIR + "llgrid.inc";
        if (!FileManagement.deleteFile(llgridFilePath)) {
            LOGGER_VARIABLE.debug("Cannot erase previous llgrid.inc because it doesn't exist.");
        }

        // Prepare files
        String fullDate = NMMBConstants.STR_TO_DATE.format(currentDate);
        String compactDate = NMMBConstants.COMPACT_STR_TO_DATE.format(currentDate);
        String hourSTR = (HOUR < 10) ? "0" + String.valueOf(HOUR) : String.valueOf(HOUR);
        String nHoursSTR = (NHOURS < 10) ? "0" + String.valueOf(NHOURS) : String.valueOf(NHOURS);

        String llgridSrcFile = NMMBEnvironment.VRB_INCLUDE_DIR + "llgrid_rrtm_" + TYPE_GFSINIT + ".tmp";
        String llgridFile = NMMBEnvironment.VRB_INCLUDE_DIR + "llgrid.inc";
        BashCMDExecutor cmdllgrid = new BashCMDExecutor("sed");
        cmdllgrid.addFlagAndValue("-e", "s/LLL/" + nHoursSTR + "/");
        cmdllgrid.addFlagAndValue("-e", "s/HH/" + hourSTR + "/");
        cmdllgrid.addFlagAndValue("-e", "s/UPBD/" + String.valueOf(BOCO) + "/");
        cmdllgrid.addFlagAndValue("-e", "s/YYYYMMDD/" + compactDate + "/");
        cmdllgrid.addArgument(llgridSrcFile);
        cmdllgrid.redirectOutput(llgridFile);
        try {
            int ev = cmdllgrid.execute();
            if (ev != 0) {
                throw new CommandException("[ERROR] CMD returned non-zero exit value: " + ev);
            }
        } catch (CommandException ce) {
            LOGGER_VARIABLE.error("[ERROR] Error performing sed command on model grid " + llgridSrcFile, ce);
            LOGGER_VARIABLE.error("Aborting...");
            System.exit(1);
        }

        if (DOMAIN) {
            if (TYPE_GFSINIT.equals(NMMBConstants.TYPE_GFSINIT_FNL)) {
                try {
                    String target = NMMBEnvironment.FNL + "fnl_" + fullDate + "_" + hourSTR + "_00";
                    String link = NMMBEnvironment.OUTPUT + "gfs.t" + hourSTR + "z.pgrbf00";
                    if (!FileManagement.deleteFile(link)) {
                        LOGGER_VARIABLE.debug("Cannot erase previous link " + link + " because it doesn't exist.");
                    }
                    Files.createSymbolicLink(Paths.get(link), Paths.get(target));
                    LOGGER_VARIABLE.debug("Softlink from : " + link + " to " + target);
                } catch (UnsupportedOperationException | IOException | SecurityException | InvalidPathException exception) {
                    LOGGER_VARIABLE.error("[ERROR] Cannot create output symlink", exception);
                    LOGGER_VARIABLE.error("Aborting...");
                    System.exit(1);
                }
            } else {
                LOGGER_VARIABLE.info("Converting wafs.00.0P5DEG from grib2 to grib1");
                String input = NMMBEnvironment.GFS + "wafs.00.0P5DEG";
                String output = NMMBEnvironment.OUTPUT + "gfs.t" + hourSTR + "z.pgrbf00";

                BashCMDExecutor cnvgrib = new BashCMDExecutor("cnvgrib");
                cnvgrib.addArgument("-g21");
                cnvgrib.addArgument(input);
                cnvgrib.addArgument(output);
                try {
                    int ev = cnvgrib.execute();
                    if (ev != 0) {
                        throw new CommandException("[ERROR] CMD returned non-zero exit value: " + ev);
                    }
                } catch (CommandException ce) {
                    LOGGER_VARIABLE.error("[ERROR] Error performing cnvgrib command", ce);
                    LOGGER_VARIABLE.error("Aborting...");
                    System.exit(1);
                }
            }
        } else {
            // Domain is 1
            if (TYPE_GFSINIT.equals(NMMBConstants.TYPE_GFSINIT_FNL)) {
                for (int i = HOUR; i < BOCO; i += NHOURS) {
                    try {
                        String iStr = (i < 10) ? "0" + String.valueOf(i) : String.valueOf(i);
                        int dDay = i / 24;
                        int hDay = i % 24;
                        String hDayStr = (hDay < 10) ? "0" + String.valueOf(hDay) : String.valueOf(hDay);
                        String dayDateStr = NMMBConstants.COMPACT_STR_TO_DATE
                                .format(currentDate.toInstant().plusSeconds(dDay * NMMBConstants.ONE_DAY_IN_SECONDS));

                        String target = NMMBEnvironment.FNL + "fnl_" + dayDateStr + "_" + hDayStr + "_00";
                        String link = NMMBEnvironment.OUTPUT + "gfs.t" + hourSTR + "z.pgrbf" + iStr;
                        if (!FileManagement.deleteFile(link)) {
                            LOGGER_VARIABLE.debug("Cannot erase previous link " + link + " because it doesn't exist.");
                        }
                        Files.createSymbolicLink(Paths.get(link), Paths.get(target));
                        LOGGER_VARIABLE.debug("Softlink from : " + link + " to " + target);
                    } catch (UnsupportedOperationException | IOException | SecurityException | InvalidPathException exception) {
                        LOGGER_VARIABLE.error("[ERROR] Cannot create output symlink", exception);
                        LOGGER_VARIABLE.error("Aborting...");
                        System.exit(1);
                    }
                }
            } else {
                for (int i = 0; i < BOCO; i += NHOURS) {
                    String iStr = (i < 10) ? "0" + String.valueOf(i) : String.valueOf(i);
                    String input = NMMBEnvironment.GFS + "wafs." + iStr + ".0P5DEG";
                    String output = NMMBEnvironment.OUTPUT + "gfs.t" + hourSTR + "z.pgrbf" + iStr;

                    BashCMDExecutor cnvgrib = new BashCMDExecutor("cnvgrib");
                    cnvgrib.addArgument("-g21");
                    cnvgrib.addArgument(input);
                    cnvgrib.addArgument(output);
                    try {
                        int ev = cnvgrib.execute();
                        if (ev != 0) {
                            throw new CommandException("[ERROR] CMD returned non-zero exit value: " + ev);
                        }
                    } catch (CommandException ce) {
                        LOGGER_VARIABLE.error("[ERROR] Error performing cnvgrib command", ce);
                        LOGGER_VARIABLE.error("Aborting...");
                        System.exit(1);
                    }
                }
            }
        }

        // Prepare modelgrid and lmimjm files
        String modelgridTMPFilePath = NMMBEnvironment.VRB_INCLUDE_DIR + "modelgrid_rrtm.tmp";
        String lmimjmTMPFilePath = NMMBEnvironment.VRB_INCLUDE_DIR + "lmimjm_rrtm.tmp";
        String modelgridFilePath = NMMBEnvironment.VRB_INCLUDE_DIR + "modelgrid.inc";
        String lmimjmFilePath = NMMBEnvironment.VRB_INCLUDE_DIR + "lmimjm.inc";

        // Clean some files
        LOGGER_VARIABLE.debug("Delete previous: " + modelgridFilePath);
        if (!FileManagement.deleteFile(modelgridFilePath)) {
            LOGGER_VARIABLE.debug("Cannot erase previous modelgrid because it doesn't exist.");
        }
        LOGGER_VARIABLE.debug("Delete previous: " + lmimjmFilePath);
        if (!FileManagement.deleteFile(lmimjmFilePath)) {
            LOGGER_VARIABLE.debug("Cannot erase previous lmimjm because it doesn't exist.");
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
            LOGGER_VARIABLE.error("[ERROR] Error performing sed command on model grid " + modelgridTMPFilePath, ce);
            LOGGER_VARIABLE.error("Aborting...");
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
            LOGGER_VARIABLE.error("[ERROR] Error performing sed command on Lmimjm " + lmimjmTMPFilePath, ce);
            LOGGER_VARIABLE.error("Aborting...");
            System.exit(1);
        }
    }

    public void postVariableExecution(String targetFolder) {
        String srcFilePath = NMMBEnvironment.VRB_INCLUDE_DIR + "lmimjm.inc";
        String targetFilePath = targetFolder + "lmimjm.inc";
        if (!FileManagement.copyFile(srcFilePath, targetFilePath)) {
            LOGGER_VARIABLE.error("[ERROR] Error copying lmimjm.inc file to " + targetFolder);
            LOGGER_VARIABLE.error("Aborting...");
            System.exit(1);
        }
    }

    /*
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ******************** UMO MODEL STEP ***************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     */

    /**
     * Actions to prepare the UMO Model Execution Step of an NMMB Execution
     * 
     * @param currentDate
     */
    public void prepareUMOMOdelExecution(Date currentDate) {
        // Copy data files
        if (NMMBEnvironment.CHEMIC == null || NMMBEnvironment.CHEMIC.isEmpty()) {
            LOGGER_UMO_MODEL.debug("Cannot copy from CHEMIC because source doesn't exist. Skipping...");
        } else {
            String dataFolderPath = NMMBEnvironment.CHEMIC + "MEGAN" + File.separator + "out" + File.separator + "aqmeii-reg"
                    + File.separator;
            File dataFolder = new File(dataFolderPath);
            File[] contentFiles = dataFolder.listFiles();
            if (contentFiles != null) {
                for (File file : contentFiles) {
                    if (file.getName().equals("isop.dat") || (file.getName().startsWith("lai") && file.getName().endsWith(".dat"))
                            || file.getName().equals("meteo-data.dat")
                            || (file.getName().startsWith("pftp_") && file.getName().endsWith(".dat"))) {

                        // Copy file
                        String targetPath = NMMBEnvironment.UMO_OUT + file.getName();
                        if (!FileManagement.copyFile(file.getAbsolutePath(), targetPath)) {
                            LOGGER_UMO_MODEL.debug("Cannot copy " + file.getName() + " file to " + targetPath
                                    + " because source doesn't exist. Skipping...");
                        }
                    }
                }
            } else {
                LOGGER_UMO_MODEL.debug("Cannot copy from CHEMIC because source doesn't exist. Skipping...");
            }
        }

        // Dust coupling part 1
        boolean coupleDustIteration = false;
        if (currentDate.after(START_DATE)) {
            coupleDustIteration = COUPLE_DUST;
        }

        if (COUPLE_DUST_INIT) {
            coupleDustIteration = true;
        }

        String dustFlag = (coupleDustIteration) ? "EEEE/true" : "EEEE/false";

        // Prepare config rrtm chem file
        String nHoursSTR = (NHOURS < 10) ? "0" + String.valueOf(NHOURS) : String.valueOf(NHOURS);
        String yearSTR = NMMBConstants.DATE_TO_YEAR.format(currentDate);
        String monthSTR = NMMBConstants.DATE_TO_MONTH.format(currentDate);
        String daySTR = NMMBConstants.DATE_TO_DAY.format(currentDate);
        String hourSTR = (HOUR < 10) ? "0" + String.valueOf(HOUR) : String.valueOf(HOUR);

        String configFileTMPPath = NMMBEnvironment.UMO_ROOT + "configfile_rrtm_chem.tmp";
        String configFilePath = NMMBEnvironment.UMO_OUT + "configure_file";
        BashCMDExecutor configFile = new BashCMDExecutor("sed");
        configFile.addFlagAndValue("-e", "s/III/" + String.valueOf(IMI) + "/");
        configFile.addFlagAndValue("-e", "s/JJJ/" + String.valueOf(JMI) + "/");
        configFile.addFlagAndValue("-e", "s/KKK/" + String.valueOf(LM) + "/");
        configFile.addFlagAndValue("-e", "s/TPHD/" + String.valueOf(TPH0D) + "/");
        configFile.addFlagAndValue("-e", "s/TLMD/" + String.valueOf(TLM0D) + "/");
        configFile.addFlagAndValue("-e", "s/WBD/" + String.valueOf(WBD) + "/");
        configFile.addFlagAndValue("-e", "s/SBD/" + String.valueOf(SBD) + "/");
        configFile.addFlagAndValue("-e", "s/INPES/" + String.valueOf(INPES) + "/");
        configFile.addFlagAndValue("-e", "s/JNPES/" + String.valueOf(JNPES) + "/");
        configFile.addFlagAndValue("-e", "s/WRTSK/" + String.valueOf(WRTSK) + "/");
        configFile.addFlagAndValue("-e", "s/DTINT/" + String.valueOf(DT_INT) + "/");
        configFile.addFlagAndValue("-e", "s/YYYY/" + yearSTR + "/");
        configFile.addFlagAndValue("-e", "s/MM/" + monthSTR + "/");
        configFile.addFlagAndValue("-e", "s/DD/" + daySTR + "/");
        configFile.addFlagAndValue("-e", "s/HH/" + hourSTR + "/");
        configFile.addFlagAndValue("-e", "s/LLL/" + nHoursSTR + "/");
        configFile.addFlagAndValue("-e", "s/STT/" + String.valueOf(HIST_M) + "/");
        configFile.addFlagAndValue("-e", "s/DOM/true/");
        configFile.addFlagAndValue("-e", "s/" + dustFlag + "/");
        configFile.addFlagAndValue("-e", "s/BBBB/" + String.valueOf(DCAL) + "/");
        configFile.addFlagAndValue("-e", "s/NRADS/" + String.valueOf(NRADS) + "/");
        configFile.addFlagAndValue("-e", "s/NRADL/" + String.valueOf(NRADL) + "/");
        configFile.addFlagAndValue("-e", "s/CCCC/" + String.valueOf(INIT_CHEM) + "/");

        configFile.addArgument(configFileTMPPath);
        configFile.redirectOutput(configFilePath);
        try {
            int ev = configFile.execute();
            if (ev != 0) {
                throw new CommandException("[ERROR] CMD returned non-zero exit value: " + ev);
            }
        } catch (CommandException ce) {
            LOGGER_VARIABLE.error("[ERROR] Error performing sed command on configFile " + configFileTMPPath, ce);
            LOGGER_VARIABLE.error("Aborting...");
            System.exit(1);
        }

        // Copy datmod
        String datModFolderPath = NMMBEnvironment.DATMOD;
        File datModFolder = new File(datModFolderPath);
        for (File file : datModFolder.listFiles()) {
            // Copy all files
            String targetPath = NMMBEnvironment.UMO_OUT + file.getName();
            if (!FileManagement.copyFile(file.getAbsolutePath(), targetPath)) {
                LOGGER_UMO_MODEL.error("[ERROR] Error copying " + file.getName() + " file to " + targetPath);
                LOGGER_UMO_MODEL.error("Aborting...");
                System.exit(1);
            }
        }

        String lookupDatSrc = NMMBEnvironment.DATMOD + "nam_micro_lookup.dat";
        String lookupDatTarget = NMMBEnvironment.UMO_OUT + "ETAMPNEW_DATA";
        if (!FileManagement.copyFile(lookupDatSrc, lookupDatTarget)) {
            LOGGER_UMO_MODEL.error("[ERROR] Error copying " + lookupDatSrc + " file to " + lookupDatTarget);
            LOGGER_UMO_MODEL.error("Aborting...");
            System.exit(1);
        }

        String wrftablesFolderPath = NMMBEnvironment.DATMOD + "wrftables" + File.separator;
        File wrftablesFolder = new File(wrftablesFolderPath);
        for (File file : wrftablesFolder.listFiles()) {
            // Copy all files
            String targetPath = NMMBEnvironment.UMO_OUT + file.getName();
            if (!FileManagement.copyFile(file.getAbsolutePath(), targetPath)) {
                LOGGER_UMO_MODEL.error("[ERROR] Error copying " + file.getName() + " file to " + targetPath);
                LOGGER_UMO_MODEL.error("Aborting...");
                System.exit(1);
            }
        }

        String co2dataFolderPath = NMMBEnvironment.DATMOD + "co2data" + File.separator;
        File co2dataFolder = new File(co2dataFolderPath);
        for (File file : co2dataFolder.listFiles()) {
            // Copy all files
            String targetPath = NMMBEnvironment.UMO_OUT + file.getName();
            if (!FileManagement.copyFile(file.getAbsolutePath(), targetPath)) {
                LOGGER_UMO_MODEL.error("[ERROR] Error copying " + file.getName() + " file to " + targetPath);
                LOGGER_UMO_MODEL.error("Aborting...");
                System.exit(1);
            }
        }

        // Copy files for RRTM radiation
        String climaGlobalSrc = NMMBEnvironment.DATMOD + "fix" + File.separator + "fix_rad" + File.separator
                + "global_climaeropac_global.txt";
        String climaGlobalTarget = NMMBEnvironment.UMO_OUT + "aerosol.dat";
        if (!FileManagement.copyFile(climaGlobalSrc, climaGlobalTarget)) {
            LOGGER_UMO_MODEL.error("[ERROR] Error copying " + climaGlobalSrc + " file to " + climaGlobalTarget);
            LOGGER_UMO_MODEL.error("Aborting...");
            System.exit(1);
        }

        String solarSrc = NMMBEnvironment.DATMOD + "fix" + File.separator + "fix_rad" + File.separator + "solarconstantdata.txt";
        String solarTarget = NMMBEnvironment.UMO_OUT + "solarconstantdata.txt";
        if (!FileManagement.copyFile(solarSrc, solarTarget)) {
            LOGGER_UMO_MODEL.error("[ERROR] Error copying " + solarSrc + " file to " + solarTarget);
            LOGGER_UMO_MODEL.error("Aborting...");
            System.exit(1);
        }

        String fixRadFolderPath = NMMBEnvironment.DATMOD + "fix" + File.separator + "fix_rad" + File.separator;
        File fixRadFolder = new File(fixRadFolderPath);
        for (File file : fixRadFolder.listFiles()) {
            if (file.getName().startsWith("co2historicaldata") || file.getName().startsWith("volcanic_aerosols_")) {
                String targetPath = NMMBEnvironment.UMO_OUT + file.getName();
                if (!FileManagement.copyFile(file.getAbsolutePath(), targetPath)) {
                    LOGGER_UMO_MODEL.error("[ERROR] Error copying " + file.getName() + " file to " + targetPath);
                    LOGGER_UMO_MODEL.error("Aborting...");
                    System.exit(1);
                }
            }
        }

        // Copy files for GoCart climatology conc. and opt. properties
        String fixGocartFolderPath = NMMBEnvironment.DATMOD + "fix" + File.separator + "fix_gocart_clim" + File.separator;
        File fixGocartFolder = new File(fixGocartFolderPath);
        for (File file : fixGocartFolder.listFiles()) {
            if (file.getName().startsWith("2000")) {
                String targetPath = NMMBEnvironment.UMO_OUT + file.getName();
                if (!FileManagement.copyFile(file.getAbsolutePath(), targetPath)) {
                    LOGGER_UMO_MODEL.error("[ERROR] Error copying " + file.getName() + " file to " + targetPath);
                    LOGGER_UMO_MODEL.error("Aborting...");
                    System.exit(1);
                }
            }
        }

        String ncepAerosolSrc = NMMBEnvironment.DATMOD + "fix" + File.separator + "fix_aeropt_luts" + File.separator + "NCEP_AEROSOL.bin";
        String ncepAerosolTarget = NMMBEnvironment.UMO_OUT + "NCEP_AEROSOL.bin";
        if (!FileManagement.copyFile(ncepAerosolSrc, ncepAerosolTarget)) {
            LOGGER_UMO_MODEL.error("[ERROR] Error copying " + ncepAerosolSrc + " file to " + ncepAerosolTarget);
            LOGGER_UMO_MODEL.error("Aborting...");
            System.exit(1);
        }

        // Copy files for chemistry tests
        // TODO: Emissions copy file from /gpfs/bsc32/BLA/NNMB/RUN/FUKU-DATA/xe133_emissions.dat
        // String emissionsSrc = "xe133_emissions.dat";
        // String emissionsTarget = NMMBEnvironment.UMO_OUT + "xe133_emissions.dat";
        // if (!FileManagement.copyFile(emissionsSrc, emissionsTarget)) {
        // LOGGER_UMO_MODEL.error("[ERROR] Error copying " + emissionsSrc + " file to " + emissionsTarget);
        // LOGGER_UMO_MODEL.error("Aborting...");
        // System.exit(1);
        // }

        String configure01Src = NMMBEnvironment.UMO_OUT + "configure_file";
        String configure01Target = NMMBEnvironment.UMO_OUT + "configure_file_01";
        if (!FileManagement.copyFile(configure01Src, configure01Target)) {
            LOGGER_UMO_MODEL.error("[ERROR] Error copying " + configure01Src + " file to " + configure01Target);
            LOGGER_UMO_MODEL.error("Aborting...");
            System.exit(1);
        }

        String modelConfigureSrc = NMMBEnvironment.UMO_OUT + "configure_file";
        String modelConfigureTarget = NMMBEnvironment.UMO_OUT + "model_configure";
        if (!FileManagement.copyFile(modelConfigureSrc, modelConfigureTarget)) {
            LOGGER_UMO_MODEL.error("[ERROR] Error copying " + modelConfigureSrc + " file to " + modelConfigureTarget);
            LOGGER_UMO_MODEL.error("Aborting...");
            System.exit(1);
        }

        String solverSrc = NMMBEnvironment.SRCDIR + "NAMELISTS" + File.separator + "solver_state.txt";
        String solverTarget = NMMBEnvironment.UMO_OUT + "solver_state.txt";
        if (!FileManagement.copyFile(solverSrc, solverTarget)) {
            LOGGER_UMO_MODEL.error("[ERROR] Error copying " + solverSrc + " file to " + solverTarget);
            LOGGER_UMO_MODEL.error("Aborting...");
            System.exit(1);
        }

        String oceanSrc = NMMBEnvironment.SRCDIR + "NAMELISTS" + File.separator + "ocean.configure";
        String oceanTarget = NMMBEnvironment.UMO_OUT + "ocean.configure";
        if (!FileManagement.copyFile(oceanSrc, oceanTarget)) {
            LOGGER_UMO_MODEL.error("[ERROR] Error copying " + oceanSrc + " file to " + oceanTarget);
            LOGGER_UMO_MODEL.error("Aborting...");
            System.exit(1);
        }

        String atmosSrc = NMMBEnvironment.SRCDIR + "NAMELISTS" + File.separator + "atmos.configure";
        String atmosTarget = NMMBEnvironment.UMO_OUT + "atmos.configure";
        if (!FileManagement.copyFile(atmosSrc, atmosTarget)) {
            LOGGER_UMO_MODEL.error("[ERROR] Error copying " + atmosSrc + " file to " + atmosTarget);
            LOGGER_UMO_MODEL.error("Aborting...");
            System.exit(1);
        }

        String globalPrdlosSrc = NMMBEnvironment.DATMOD + "global_o3prdlos.f77";
        String globalPrdlosTarget = NMMBEnvironment.UMO_OUT + "fort.28";
        try {
            Files.createSymbolicLink(Paths.get(globalPrdlosTarget), Paths.get(globalPrdlosSrc));
        } catch (UnsupportedOperationException | IOException | SecurityException | InvalidPathException exception) {
            LOGGER_UMO_MODEL.error("[ERROR] Cannot create symlink " + globalPrdlosTarget + " from " + globalPrdlosSrc, exception);
            LOGGER_UMO_MODEL.error("Aborting...");
            System.exit(1);
        }

        String globalClimSrc = NMMBEnvironment.DATMOD + "global_o3clim.txt";
        String globalClimTarget = NMMBEnvironment.UMO_OUT + "fort.48";
        try {
            Files.createSymbolicLink(Paths.get(globalClimTarget), Paths.get(globalClimSrc));
        } catch (UnsupportedOperationException | IOException | SecurityException | InvalidPathException exception) {
            LOGGER_UMO_MODEL.error("[ERROR] Cannot create symlink " + globalClimTarget + " from " + globalClimSrc, exception);
            LOGGER_UMO_MODEL.error("Aborting...");
            System.exit(1);
        }
    }

    /**
     * Actions to perform after the UMO Model Execution
     * 
     * @param currentDate
     */
    public void postUMOModelExecution(Date currentDate) {
        // Define model output folder by case and date
        String currentDateSTR = NMMBConstants.STR_TO_DATE.format(currentDate);
        String hourSTR = (HOUR < 10) ? "0" + String.valueOf(HOUR) : String.valueOf(HOUR);
        String folderOutputCase = NMMBEnvironment.OUTNMMB + CASE + File.separator;
        String folderOutput = NMMBEnvironment.OUTNMMB + CASE + File.separator + currentDateSTR + hourSTR + File.separator;

        String historyFilePath = NMMBEnvironment.UMO_OUT + "history_INIT.hhh";
        FileManagement.deleteFile(historyFilePath);

        if (COUPLE_DUST) {
            String historyTarget = folderOutputCase + "history_INIT.hhh";
            String historySrc;
            if (NHOURS_INIT < 100) {
                if (NHOURS_INIT < 10) {
                    historySrc = NMMBEnvironment.UMO_OUT + "nmmb_hst_01_bin_000" + String.valueOf(NHOURS_INIT) + "h_00m_00.00s";
                } else {
                    historySrc = NMMBEnvironment.UMO_OUT + "nmmb_hst_01_bin_00" + String.valueOf(NHOURS_INIT) + "h_00m_00.00s";
                }
            } else {
                historySrc = NMMBEnvironment.UMO_OUT + "nmmb_hst_01_bin_0" + String.valueOf(NHOURS_INIT) + "h_00m_00.00s";
            }

            if (!FileManagement.copyFile(historySrc, historyTarget)) {
                LOGGER_UMO_MODEL.error("[ERROR] Error copying " + historySrc + " file to " + historyTarget);
                LOGGER_UMO_MODEL.error("Aborting...");
                System.exit(1);
            }
        }

        String nmmRrtmOutSrc = NMMBEnvironment.UMO_OUT + "nmm_rrtm.out";
        String nmmRrtmOutTarget = folderOutput + "nmm_rrtm.out";
        if (!FileManagement.moveFile(nmmRrtmOutSrc, nmmRrtmOutTarget)) {
            // TODO: We don't really need to abort when cannot copy this file?
            LOGGER_UMO_MODEL.error("Cannot copy " + nmmRrtmOutSrc + " file to " + nmmRrtmOutTarget);
        }

        String configureFileSrc = NMMBEnvironment.UMO_OUT + "configure_file";
        String configureFileTarget = folderOutput + "configure_file";
        if (!FileManagement.moveFile(configureFileSrc, configureFileTarget)) {
            LOGGER_UMO_MODEL.error("[ERROR] Error copying " + configureFileSrc + " file to " + configureFileTarget);
            LOGGER_UMO_MODEL.error("Aborting...");
            System.exit(1);
        }

        String boundarySrc = NMMBEnvironment.OUTPUT + "boundary_ecmwf.nc";
        String boundaryTarget = folderOutput + "boundary_ecmwf.nc";
        if (!FileManagement.moveFile(boundarySrc, boundaryTarget)) {
            // TODO: We don't really need to abort when cannot copy this file?
            LOGGER_UMO_MODEL.warn("Cannot copy " + boundarySrc + " file to " + boundaryTarget);
        }

        File umoFolder = new File(NMMBEnvironment.UMO_OUT);
        for (File file : umoFolder.listFiles()) {
            if (file.getName().startsWith("nmmb_hst_01_bin_")) {
                String target = folderOutput + file.getName();
                if (!FileManagement.moveFile(file.getAbsolutePath(), target)) {
                    LOGGER_UMO_MODEL.error("[ERROR] Error copying " + file.getName() + " file to " + target);
                    LOGGER_UMO_MODEL.error("Aborting...");
                    System.exit(1);
                }
            }
        }
    }

    /*
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ******************** POST PROCESS STEP ************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     */

    /**
     * Actions to perform on the POST PROCESS Step of an NMMB Execution
     * 
     * @param currentDate
     */
    public void preparePostProcessExecution(Date currentDate) {
        // Define model output folder by case and date
        String currentDateSTR = NMMBConstants.STR_TO_DATE.format(currentDate);
        String hourSTR = (HOUR < 10) ? "0" + String.valueOf(HOUR) : String.valueOf(HOUR);
        String folderOutputCase = NMMBEnvironment.OUTNMMB + CASE + File.separator;
        String folderOutput = NMMBEnvironment.OUTNMMB + CASE + File.separator + currentDateSTR + hourSTR + File.separator;

        String lmimjmSrc = folderOutputCase + "lmimjm.inc";
        String lmimjmTarget = NMMBEnvironment.POST_CARBONO + "lmimjm.inc";
        if (!FileManagement.copyFile(lmimjmSrc, lmimjmTarget)) {
            LOGGER_POST.error("[ERROR] Error copying " + lmimjmSrc + " file to " + lmimjmTarget);
            LOGGER_POST.error("Aborting...");
            System.exit(1);
        }

        String postAllSrc = NMMBEnvironment.POST_CARBONO + "new_postall.f.tmp";
        String postAllTarget = NMMBEnvironment.POST_CARBONO + "new_postall.f";
        String hourPSTR = (HOUR_P < 10) ? "0" + String.valueOf(HOUR_P) : String.valueOf(HOUR_P);
        String nHoursPSTR = (NHOURS_P < 10) ? "0" + String.valueOf(NHOURS_P) : String.valueOf(NHOURS_P);
        BashCMDExecutor cmdPostall = new BashCMDExecutor("sed");
        cmdPostall.addFlagAndValue("-e", "s/QQQ/" + nHoursPSTR + "/");
        cmdPostall.addFlagAndValue("-e", "s/SSS/" + hourPSTR + "/");
        cmdPostall.addFlagAndValue("-e", "s/TTT/" + String.valueOf(HIST_P) + "/");
        cmdPostall.addArgument(postAllSrc);
        cmdPostall.redirectOutput(postAllTarget);
        try {
            int ev = cmdPostall.execute();
            if (ev != 0) {
                throw new CommandException("[ERROR] CMD returned non-zero exit value: " + ev);
            }
        } catch (CommandException ce) {
            LOGGER_POST.error("[ERROR] Error performing sed command on " + postAllSrc, ce);
            LOGGER_POST.error("Aborting...");
            System.exit(1);
        }

        String datePost = NMMBConstants.MONTH_NAME_DATE_TO_STR.format(currentDate);
        int tdeft = (int) (NHOURS_P / (HIST + 1));
        String tdef = (tdeft < 10) ? "0" + String.valueOf(tdeft) : String.valueOf(tdeft);

        if (DOMAIN) {
            String poutGlobalCtlSrc = NMMBEnvironment.POST_CARBONO + "pout_global_pressure.ctl.tmp";
            String poutGlobalCtlTarget = folderOutput + "pout_global_pressure_" + currentDateSTR + hourSTR + ".ctl";
            BashCMDExecutor cmdGlobalCtl = new BashCMDExecutor("sed");
            cmdGlobalCtl.addFlagAndValue("-e", "s/DATE/" + currentDateSTR + hourSTR + "/");
            cmdGlobalCtl.addFlagAndValue("-e", "s/III/" + String.valueOf(IMI) + "/");
            cmdGlobalCtl.addFlagAndValue("-e", "s/WBDN/" + String.valueOf(WBD) + "/");
            cmdGlobalCtl.addFlagAndValue("-e", "s/DLMN/" + String.valueOf(DLMD) + "/");
            cmdGlobalCtl.addFlagAndValue("-e", "s/JJJ/" + String.valueOf(JMI) + "/");
            cmdGlobalCtl.addFlagAndValue("-e", "s/SBDN/" + String.valueOf(SBD) + "/");
            cmdGlobalCtl.addFlagAndValue("-e", "s/DPHN/" + String.valueOf(DPHD) + "/");
            cmdGlobalCtl.addFlagAndValue("-e", "s/KKK/" + String.valueOf(LSM) + "/");
            cmdGlobalCtl.addFlagAndValue("-e", "s/HH/" + tdef + "/");
            cmdGlobalCtl.addFlagAndValue("-e", "s/INITCTL/" + HOUR + "Z" + datePost + "/");
            cmdGlobalCtl.addFlagAndValue("-e", "s/XHR/" + String.valueOf(HIST_P) + "hr/");
            cmdGlobalCtl.addArgument(poutGlobalCtlSrc);
            cmdGlobalCtl.redirectOutput(poutGlobalCtlTarget);
            try {
                int ev = cmdGlobalCtl.execute();
                if (ev != 0) {
                    throw new CommandException("[ERROR] CMD returned non-zero exit value: " + ev);
                }
            } catch (CommandException ce) {
                LOGGER_POST.error("[ERROR] Error performing sed command on " + poutGlobalCtlSrc, ce);
                LOGGER_POST.error("Aborting...");
                System.exit(1);
            }
        } else {
            int ireg = IMI - 2;
            int jreg = JMI - 2;
            String poutRegionalCtlSrc = NMMBEnvironment.POST_CARBONO + "pout_regional_pressure.ctl.tmp";
            String poutRegionalCtlTarget = folderOutput + "pout_regional_pressure_" + currentDateSTR + hourSTR + ".ctl";
            BashCMDExecutor cmdRegionalCtl = new BashCMDExecutor("sed");
            cmdRegionalCtl.addFlagAndValue("-e", "s/DATE/" + currentDateSTR + hourSTR + "/");
            cmdRegionalCtl.addFlagAndValue("-e", "s/IRG/" + String.valueOf(ireg) + "/");
            cmdRegionalCtl.addFlagAndValue("-e", "s/JRG/" + String.valueOf(jreg) + "/");
            cmdRegionalCtl.addFlagAndValue("-e", "s/TLMN/" + String.valueOf(TLM0D) + "/");
            cmdRegionalCtl.addFlagAndValue("-e", "s/TPHN/" + String.valueOf(TPH0DN) + "/");
            cmdRegionalCtl.addFlagAndValue("-e", "s/DLMN/" + String.valueOf(DLMD) + "/");
            cmdRegionalCtl.addFlagAndValue("-e", "s/DPHN/" + String.valueOf(DPHD) + "/");
            cmdRegionalCtl.addFlagAndValue("-e", "s/WBDN/" + String.valueOf(WBD) + "/");
            cmdRegionalCtl.addFlagAndValue("-e", "s/SBDN/" + String.valueOf(SBD) + "/");
            cmdRegionalCtl.addFlagAndValue("-e", "s/III/" + String.valueOf(IMI) + "/");
            cmdRegionalCtl.addFlagAndValue("-e", "s/JJJ/" + String.valueOf(JMI) + "/");
            cmdRegionalCtl.addFlagAndValue("-e", "s/WBXX/" + String.valueOf(WBDDEF) + "/");
            cmdRegionalCtl.addFlagAndValue("-e", "s/SBYY/" + String.valueOf(SBDDEF) + "/");
            cmdRegionalCtl.addFlagAndValue("-e", "s/KKK/" + String.valueOf(LSM) + "/");
            cmdRegionalCtl.addFlagAndValue("-e", "s/HH/" + tdef + "/");
            cmdRegionalCtl.addFlagAndValue("-e", "s/INITCTL/" + HOUR + "Z" + datePost + "/");
            cmdRegionalCtl.addFlagAndValue("-e", "s/XHR/" + String.valueOf(HIST_P) + "hr/");
            cmdRegionalCtl.addArgument(poutRegionalCtlSrc);
            cmdRegionalCtl.redirectOutput(poutRegionalCtlTarget);
            try {
                int ev = cmdRegionalCtl.execute();
                if (ev != 0) {
                    throw new CommandException("[ERROR] CMD returned non-zero exit value: " + ev);
                }
            } catch (CommandException ce) {
                LOGGER_POST.error("[ERROR] Error performing sed command on " + poutRegionalCtlSrc, ce);
                LOGGER_POST.error("Aborting...");
                System.exit(1);
            }
        }
    }

}
