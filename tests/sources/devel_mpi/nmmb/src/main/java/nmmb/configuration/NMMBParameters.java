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
    private static final Logger LOGGER_VARIABLE = LogManager.getLogger(LoggerNames.NMMB_VARIABLE);

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
    public int HOUR;
    public int NHOURS;
    public int BOCO;
    public String TYPE_GFSINIT;

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
        HOUR = nmmbConfiguration.getHour();
        NHOURS = nmmbConfiguration.getNHours();
        BOCO = nmmbConfiguration.getBoco();
        TYPE_GFSINIT = nmmbConfiguration.getTypeGFSInit();

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
                    String link = NMMBEnvironment.FNL + "fnl_" + fullDate + "_" + hourSTR + "_00";
                    String target = NMMBEnvironment.OUTPUT + "gfs.t" + hourSTR + "z.pgrbf00";
                    if (!FileManagement.deleteFile(link)) {
                        LOGGER_VARIABLE.debug("Cannot erase previous link " + link + " because it doesn't exist.");
                    }
                    Files.createSymbolicLink(Paths.get(link), Paths.get(target));
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

                        String link = NMMBEnvironment.FNL + "fnl_" + dayDateStr + "_" + hDayStr + "_00";
                        String target = NMMBEnvironment.OUTPUT + "gfs.t" + hourSTR + "z.pgrbf" + iStr;
                        if (!FileManagement.deleteFile(link)) {
                            LOGGER_VARIABLE.debug("Cannot erase previous link " + link + " because it doesn't exist.");
                        }
                        Files.createSymbolicLink(Paths.get(link), Paths.get(target));
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

}
