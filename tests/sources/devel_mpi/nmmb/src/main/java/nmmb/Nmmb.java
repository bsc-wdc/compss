package nmmb;

import java.io.File;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import binary.BINARY;
import fixed.utils.FortranWrapper;
import fixed.utils.MessagePrinter;
import nmmb.constants.NMMBConstants;
import nmmb.exceptions.CommandException;
import nmmb.loggers.LoggerNames;
import nmmb.utils.BashCMDExecutor;
import nmmb.utils.FileManagement;
import nmmb.utils.NMMBConfigManager;


public class Nmmb {

    // Loggers
    private static final Logger LOGGER_MAIN = LogManager.getLogger(LoggerNames.NMMB_MAIN);
    private static final Logger LOGGER_FIXED = LogManager.getLogger(LoggerNames.NMMB_FIXED);
    private static final Logger LOGGER_VARIABLE = LogManager.getLogger(LoggerNames.NMMB_VARIABLE);
    private static final Logger LOGGER_UMO_MODEL = LogManager.getLogger(LoggerNames.NMMB_UMO_MODEL);
    private static final Logger LOGGER_POST = LogManager.getLogger(LoggerNames.NMMB_POST);

    // NMMB Configuration Manager
    private static NMMBConfigManager NMMB_CONFIGURATION;

    // -----------------------------------------------------------------------
    // MN settings
    private static int INPES;
    private static int JNPES;
    private static int WRTSK;
    private static int PROC;

    // -----------------------------------------------------------------------
    // Global-regional switch - Model domain setup global/regional
    private static boolean DOMAIN;
    private static int LM;
    private static String CASE;

    // -----------------------------------------------------------------------
    // Model variables
    private static int DT_INT;
    private static double TLM0D;
    private static double TPH0D;
    private static double WBD;
    private static double SBD;
    private static double DLMD;
    private static double DPHD;
    private static double PTOP;
    private static double DCAL;
    private static int NRADS;
    private static int NRADL;
    private static int IMI;
    private static int JMI;
    private static int IM;
    private static int JM;

    // -----------------------------------------------------------------------
    // Case selection
    private static boolean DO_FIXED;
    private static boolean DO_VRBL;
    private static boolean DO_UMO;
    private static boolean DO_POST;

    // -----------------------------------------------------------------------
    // Select START and ENDING Times
    private static Date START_DATE;
    private static Date END_DATE;

    // -----------------------------------------------------------------------
    // Select configuration of POSTPROC (DO_POST)

    // -----------------------------------------------------------------------
    // Select IC of chemistry for run with COUPLE_DUST_INIT=0
    private static int INIT_CHEM;

    // -----------------------------------------------------------------------
    // Couple dust

    // -----------------------------------------------------------------------
    // Retrieve information from environment
    private static final String UMO_PATH = System.getenv(NMMBConstants.ENV_NAME_UMO_PATH) + File.separator;

    private static final String OUTNMMB = System.getenv(NMMBConstants.ENV_NAME_OUTNMMB) + File.separator;
    private static final String UMO_OUT = System.getenv(NMMBConstants.ENV_NAME_UMO_OUT) + File.separator;

    private static final String FIX = System.getenv(NMMBConstants.ENV_NAME_FIX) + File.separator;
    public static final String FIX_FOR_ITF = "/home/bsc19/bsc19533/nmmb/nmmb-bsc-ctm-v2.0/PREPROC/FIXED/";
    private static final String LOOKUP_TABLES_DIR = FIX + "lookup_tables" + File.separator;
    private static final String GEODATA_DIR = FIX + "geodata" + File.separator;
    private static final String GTOPO_DIR = FIX + "GTOPO30" + File.separator;
    private static final String OUTPUT = System.getenv(NMMBConstants.ENV_NAME_OUTPUT);

    // -----------------------------------------------------------------------
    // Format conversion variables
    private static final SimpleDateFormat STR_TO_DATE = new SimpleDateFormat("yyyyMMdd");
    private static final long ONE_DAY_IN_SECONDS = 1 * 24 * 60 * 60;

    // -----------------------------------------------------------------------
    // Execution variables
    private static Date CURRENT_DATE;
    private static String FOLDER_OUTPUT_CASE;
    private static String FOLDER_OUTPUT;


    /**
     * MAIN NMMB WORKFLOW
     * 
     * @param args
     *            args[0] : Configuration file path
     * 
     */
    public static void main(String[] args) {
        LOGGER_MAIN.info("Starting NMMB application");

        // Check and get arguments
        if (args.length != 1) {
            usage();
            System.exit(1);
        }
        String configurationFile = args[0];

        // Load configuration
        try {
            loadConfiguration(configurationFile);
        } catch (ConfigurationException ce) {
            LOGGER_MAIN.error("[ERROR] Cannot load configuration file: " + configurationFile, ce);
            LOGGER_MAIN.error("Aborting...");
            System.exit(1);
        }

        // Compute the execution variables
        setExecutionVariables();

        // Prepare the execution
        prepareExecution();

        // Fixed process (do before main time looping)
        if (DO_FIXED) {
            doFixed();
        }

        // Start main time loop
        while (!CURRENT_DATE.after(END_DATE)) {
            String currentDateSTR = STR_TO_DATE.format(CURRENT_DATE);
            LOGGER_MAIN.info(currentDateSTR + " simulation started");

            // Define model output folder by case and date
            FOLDER_OUTPUT_CASE = OUTNMMB + File.separator + CASE;
            // FOLDER_OUTPUT = OUTNMMB + File.separator + CASE + File.separator + STR_TO_DATE.format(CURRENT_DATE) +
            // HOUR;

            // Vrbl process
            if (DO_VRBL) {
                doVariable();
            }

            // UMO model run
            if (DO_UMO) {
                doUMOModel();
            }

            // Post process
            if (DO_POST) {
                doPost();
            }

            LOGGER_MAIN.info(currentDateSTR + " simulation finished");

            // Getting next simulation day
            CURRENT_DATE = Date.from(CURRENT_DATE.toInstant().plusSeconds(ONE_DAY_IN_SECONDS));
        }
    }

    private static void loadConfiguration(String configurationFile) throws ConfigurationException {
        LOGGER_MAIN.info("Loading NMMB Configuration file " + configurationFile);
        NMMB_CONFIGURATION = new NMMBConfigManager(configurationFile);
        LOGGER_MAIN.info("Configuration file loaded");
    }

    private static void usage() {
        LOGGER_MAIN.info("Invalid parameters for nmmb.Nmmb");
        LOGGER_MAIN.info("    Usage: nmmb.Nmmb <configFilePath>");
    }

    private static void setExecutionVariables() {
        LOGGER_MAIN.info("Setting execution variables...");

        // MN settings
        INPES = NMMB_CONFIGURATION.getINPES();
        JNPES = NMMB_CONFIGURATION.getJNPES();
        WRTSK = NMMB_CONFIGURATION.getWRTSK();
        PROC = INPES * JNPES + WRTSK;

        // Global-regional switch - Model domain setup global/regional
        DOMAIN = NMMB_CONFIGURATION.getDomain();
        LM = NMMB_CONFIGURATION.getLM();
        CASE = NMMB_CONFIGURATION.getCase();

        // Model variables
        DT_INT = (DOMAIN) ? NMMB_CONFIGURATION.getDT_INT1() : NMMB_CONFIGURATION.getDT_INT2();
        TLM0D = (DOMAIN) ? NMMB_CONFIGURATION.getTLM0D1() : NMMB_CONFIGURATION.getTLM0D2();
        TPH0D = (DOMAIN) ? NMMB_CONFIGURATION.getTPH0D1() : NMMB_CONFIGURATION.getTPH0D2();
        WBD = (DOMAIN) ? NMMB_CONFIGURATION.getWBD1() : NMMB_CONFIGURATION.getWBD2();
        SBD = (DOMAIN) ? NMMB_CONFIGURATION.getSBD1() : NMMB_CONFIGURATION.getSBD2();
        DLMD = (DOMAIN) ? NMMB_CONFIGURATION.getDLMD1() : NMMB_CONFIGURATION.getDLMD2();
        DPHD = (DOMAIN) ? NMMB_CONFIGURATION.getDPHD1() : NMMB_CONFIGURATION.getDPHD2();
        PTOP = (DOMAIN) ? NMMB_CONFIGURATION.getPTOP1() : NMMB_CONFIGURATION.getPTOP2();
        DCAL = (DOMAIN) ? NMMB_CONFIGURATION.getDCAL1() : NMMB_CONFIGURATION.getDCAL2();
        NRADS = (DOMAIN) ? NMMB_CONFIGURATION.getNRADS1() : NMMB_CONFIGURATION.getNRADS2();
        NRADL = (DOMAIN) ? NMMB_CONFIGURATION.getNRADL1() : NMMB_CONFIGURATION.getNRADL2();
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
        DO_FIXED = NMMB_CONFIGURATION.getFixed();
        DO_VRBL = NMMB_CONFIGURATION.getVariable();
        DO_UMO = NMMB_CONFIGURATION.getUmoModel();
        DO_POST = NMMB_CONFIGURATION.getPost();

        // -----------------------------------------------------------------------
        // Select START and ENDING Times
        try {
            START_DATE = STR_TO_DATE.parse(NMMB_CONFIGURATION.getStartDate());
        } catch (ParseException pe) {
            LOGGER_MAIN.error("[ERROR] Cannot parse start date", pe);
            LOGGER_MAIN.error("Aborting...");
            System.exit(1);
        }
        CURRENT_DATE = START_DATE;
        try {
            END_DATE = STR_TO_DATE.parse(NMMB_CONFIGURATION.getEndDate());
        } catch (ParseException pe) {
            LOGGER_MAIN.error("[ERROR] Cannot parse end date", pe);
            LOGGER_MAIN.error("Aborting...");
            System.exit(1);
        }

        // -----------------------------------------------------------------------
        // Select configuration of POSTPROC (DO_POST)

        // -----------------------------------------------------------------------
        // Select IC of chemistry for run with COUPLE_DUST_INIT=0
        INIT_CHEM = NMMB_CONFIGURATION.getInitChem();

        LOGGER_MAIN.info("Execution variables set");
    }

    private static void prepareExecution() {
        LOGGER_MAIN.info("Preparing execution...");

        // Define folders
        String outputPath = UMO_OUT;
        String outputCasePath = OUTNMMB + CASE + File.separator + "output" + File.separator;
        String outputSymPath = UMO_PATH + "PREPROC" + File.separator + "output";

        // Clean folders
        LOGGER_MAIN.debug("Clean output folder : " + outputPath);
        FileManagement.deleteAll(new File(outputPath));
        LOGGER_MAIN.debug("Clean output folder : " + outputCasePath);
        FileManagement.deleteAll(new File(outputCasePath));
        LOGGER_MAIN.debug("Clean output folder : " + outputSymPath);
        FileManagement.deleteAll(new File(outputSymPath));

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
        } catch (IOException ioe) {
            LOGGER_MAIN.error("[ERROR] Cannot create output symlink", ioe);
            LOGGER_MAIN.error("Aborting...");
            System.exit(1);
        }

        LOGGER_MAIN.info("Execution environment prepared");
    }

    private static void doFixed() {
        LOGGER_FIXED.info("Enter fixed process");

        String modelgridTMPFilePath = FIX + "include" + File.separator + "modelgrid_rrtm.tmp";
        String lmimjmTMPFilePath = FIX + "include" + File.separator + "lmimjm_rrtm.tmp";
        String modelgridFilePath = FIX + "include" + File.separator + "modelgrid.inc";
        String lmimjmFilePath = FIX + "include" + File.separator + "lmimjm.inc";

        // Clean some files
        LOGGER_FIXED.debug("Delete previous: " + modelgridFilePath);
        //TODO: FileManagement.deleteAll(new File(modelgridFilePath));
        LOGGER_FIXED.debug("Delete previous: " + lmimjmFilePath);
        //TODO: FileManagement.deleteAll(new File(lmimjmFilePath));

        // Prepare files
        BashCMDExecutor cmdModelgrid = new BashCMDExecutor("sed");
        cmdModelgrid.addFlagAndValue("-e", "s/TLMD/" + TLM0D + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/TPHD/" + TPH0D + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/WBDN/" + WBD + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/SBDN/" + SBD + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/DLMN/" + DLMD + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/DPHN/" + DPHD + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/III/" + IMI + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/JJJ/" + JMI + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/IBDY/" + IM + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/JBDY/" + JM + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/PTOP/" + PTOP + "/");
        cmdModelgrid.addFlagAndValue("-e", "s/KKK/" + LM + "/");
        cmdModelgrid.addArgument(modelgridTMPFilePath);
        cmdModelgrid.addArgument(">");
        cmdModelgrid.addArgument(modelgridFilePath);
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
        cmdLmimjm.addFlagAndValue("-e", "s/TLMD/" + TLM0D + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/TPHD/" + TPH0D + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/WBDN/" + WBD + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/SBDN/" + SBD + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/DLMN/" + DLMD + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/DPHN/" + DPHD + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/III/" + IMI + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/JJJ/" + JMI + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/IBDY/" + IM + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/JBDY/" + JM + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/PTOP/" + PTOP + "/");
        cmdLmimjm.addFlagAndValue("-e", "s/KKK/" + LM + "/");
        cmdLmimjm.addArgument(lmimjmTMPFilePath);
        cmdLmimjm.addArgument(">");
        cmdLmimjm.addArgument(lmimjmFilePath);
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

        /*
         * ***************************************************************************************************
         * ***************************************************************************************************
         * ***************************************************************************************************
         * ******************** FIXED STEP *******************************************************************
         * ***************************************************************************************************
         * ***************************************************************************************************
         * ***************************************************************************************************
         */

        /* Build the fortran executables *************************************************/
        MessagePrinter.printInfoMsg("Building fixed executables");
        for (String fortranFile : FortranWrapper.FORTRAN_F90_FILES) {
            String executable = FIX + fortranFile + FortranWrapper.SUFFIX_EXE;
            String src = FIX + fortranFile + FortranWrapper.SUFFIX_F90_SRC;
            BINARY.fortranCompiler(FortranWrapper.FFLAGS, FortranWrapper.OFLAG, executable, src);
        }
        for (String fortranFile : FortranWrapper.FORTRAN_F_FILES) {
            String executable = FIX + fortranFile + FortranWrapper.SUFFIX_EXE;
            String src = FIX + fortranFile + FortranWrapper.SUFFIX_F_SRC;
            BINARY.fortranCompiler(FortranWrapper.FFLAGS, FortranWrapper.OFLAG, executable, src);
        }
        MessagePrinter.printInfoMsg("Finished building fixed executables");

        /* Begin binary calls ***********************************************************/
        MessagePrinter.printHeaderMsg("BEGIN");

        MessagePrinter.printInfoMsg("Generate DEM height and sea mask files");
        String topoDir = GEODATA_DIR + "topo1kmDEM" + File.separator;
        String seamaskDEM = OUTPUT + "seamaskDEM";
        String heightDEM = OUTPUT + "heightDEM";
        BINARY.smmount(topoDir, seamaskDEM, heightDEM);

        MessagePrinter.printInfoMsg("Generate landuse file");
        String landuse = OUTPUT + "landuse";
        String kount_landuse = OUTPUT + "kount_landuse";
        BINARY.landuse(GTOPO_DIR, landuse, kount_landuse);

        MessagePrinter.printInfoMsg("Generate landusenew file");
        String gtopDir = GEODATA_DIR + "landuse_30s" + File.separator;
        String landusenew = OUTPUT + "landusenew";
        String kount_landusenew = OUTPUT + "kount_landusenew";
        BINARY.landusenew(gtopDir, landusenew, kount_landusenew);

        MessagePrinter.printInfoMsg("Generate mountains");
        String topo30sDir = GEODATA_DIR + "topo_30s" + File.separator;
        String heightmean = OUTPUT + "heightmean";
        BINARY.topo(topo30sDir, heightmean);

        MessagePrinter.printInfoMsg("Generate standard deviation of topography height");
        String stdh = OUTPUT + "stdh";
        BINARY.stdh(heightmean, seamaskDEM, topo30sDir, stdh);

        MessagePrinter.printInfoMsg("Generate envelope mountains");
        String height = OUTPUT + "height";
        BINARY.envelope(heightmean, stdh, height);

        MessagePrinter.printInfoMsg("Generate top soil type file");
        String soiltypeDir = GEODATA_DIR + "soiltype_top_30s" + File.separator;
        String topsoiltype = OUTPUT + "topsoiltype";
        BINARY.topsoiltype(seamaskDEM, soiltypeDir, topsoiltype);

        MessagePrinter.printInfoMsg("Generate bottom soil type file");
        String soiltypePath = GEODATA_DIR + "soiltype_bot_30s" + File.separator;
        String botsoiltype = OUTPUT + "botsoiltype";
        BINARY.botsoiltype(seamaskDEM, soiltypePath, botsoiltype);

        MessagePrinter.printInfoMsg("Generate sea mask and reprocess mountains");
        String seamask = OUTPUT + "seamask";
        BINARY.toposeamask(seamaskDEM, seamask, height, landuse, topsoiltype, botsoiltype);

        MessagePrinter.printInfoMsg("Reprocess standard deviation of topography height");
        BINARY.stdhtopo(seamask, stdh);

        MessagePrinter.printInfoMsg("Generate deep soil temperature");
        String soiltempPath = GEODATA_DIR + "soiltemp_1deg" + File.separator;
        String deeptemperature = OUTPUT + "deeptemperature";
        BINARY.deeptemperature(seamask, soiltempPath, deeptemperature);

        MessagePrinter.printInfoMsg("Generate maximum snow albedo");
        String maxsnowalbDir = GEODATA_DIR + "maxsnowalb" + File.separator;
        String snowalbedo = OUTPUT + "snowalbedo";
        BINARY.snowalbedo(maxsnowalbDir, snowalbedo);

        MessagePrinter.printInfoMsg("Generate vertical coordinate");
        String dsg = OUTPUT + "dsg";
        BINARY.vcgenerator(dsg);

        MessagePrinter.printInfoMsg("Generate highres roughness length for africa and asia");
        String roughnessDir = GEODATA_DIR + "roughness_025s" + File.separator;
        String roughness = OUTPUT + "roughness";
        BINARY.roughness(roughnessDir, roughness);

        MessagePrinter.printInfoMsg("Generate co2 files");
        String co2_data_dir = GEODATA_DIR + "co2data" + File.separator;
        String co2_trans = OUTPUT + "co2_trans";
        BINARY.gfdlco2(dsg, co2_data_dir, co2_trans);

        MessagePrinter.printInfoMsg("Generate lookup tables for aerosol scavenging collection efficiencies");
        String lookup_aerosol2_rh00 = OUTPUT + "lookup_aerosol2.dat.rh00";
        String lookup_aerosol2_rh50 = OUTPUT + "lookup_aerosol2.dat.rh50";
        String lookup_aerosol2_rh70 = OUTPUT + "lookup_aerosol2.dat.rh70";
        String lookup_aerosol2_rh80 = OUTPUT + "lookup_aerosol2.dat.rh80";
        String lookup_aerosol2_rh90 = OUTPUT + "lookup_aerosol2.dat.rh90";
        String lookup_aerosol2_rh95 = OUTPUT + "lookup_aerosol2.dat.rh95";
        String lookup_aerosol2_rh99 = OUTPUT + "lookup_aerosol2.dat.rh99";
        BINARY.run_aerosol(lookup_aerosol2_rh00, lookup_aerosol2_rh50, lookup_aerosol2_rh70, lookup_aerosol2_rh80, lookup_aerosol2_rh90,
                lookup_aerosol2_rh95, lookup_aerosol2_rh99);

        /* Clean Up binaries ************************************************************/
        MessagePrinter.printInfoMsg("Clean up executables");
        for (String fortranFile : FortranWrapper.FORTRAN_F90_FILES) {
            String executable = FIX + fortranFile + FortranWrapper.SUFFIX_EXE;
            File f = new File(executable);
            if (f.exists()) {
                f.delete();
            }
        }
        for (String fortranFile : FortranWrapper.FORTRAN_F_FILES) {
            String executable = FIX + fortranFile + FortranWrapper.SUFFIX_EXE;
            File f = new File(executable);
            if (f.exists()) {
                f.delete();
            }
        }

        /* End binary calls *************************************************************/
        MessagePrinter.printHeaderMsg("END");

        LOGGER_FIXED.info("Fixed process finished");
    }

    private static void doVariable() {
        LOGGER_VARIABLE.info("Enter variable process");
        LOGGER_VARIABLE.info("Variable process finished");
    }

    private static void doUMOModel() {
        LOGGER_UMO_MODEL.info("Enter UMO Model process");
        LOGGER_UMO_MODEL.info("UMO Model process finished");
    }

    private static void doPost() {
        LOGGER_POST.info("Enter post process");
        LOGGER_POST.info("Post process finished");
    }

}
