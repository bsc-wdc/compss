package nmmb;

import java.io.File;

import java.util.Date;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import binary.BINARY;
import nmmb.configuration.NMMBConfigManager;
import nmmb.configuration.NMMBConstants;
import nmmb.configuration.NMMBEnvironment;
import nmmb.configuration.NMMBParameters;
import nmmb.fixed.utils.FortranWrapper;
import nmmb.loggers.LoggerNames;
import nmmb.utils.MessagePrinter;


public class Nmmb {

    // Loggers
    private static final Logger LOGGER_MAIN = LogManager.getLogger(LoggerNames.NMMB_MAIN);
    private static final Logger LOGGER_FIXED = LogManager.getLogger(LoggerNames.NMMB_FIXED);
    private static final Logger LOGGER_VARIABLE = LogManager.getLogger(LoggerNames.NMMB_VARIABLE);
    private static final Logger LOGGER_UMO_MODEL = LogManager.getLogger(LoggerNames.NMMB_UMO_MODEL);
    private static final Logger LOGGER_POST = LogManager.getLogger(LoggerNames.NMMB_POST);


    /**
     * Prints the usage
     * 
     */
    private static void usage() {
        LOGGER_MAIN.info("Invalid parameters for nmmb.Nmmb");
        LOGGER_MAIN.info("    Usage: nmmb.Nmmb <configFilePath>");
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
    private static void doFixed(NMMBParameters nmmbParams) {
        LOGGER_FIXED.info("Enter fixed process");

        /* Prepare execution **************************************************************/
        nmmbParams.prepareFixedExecution();

        /* Build the fortran executables *************************************************/
        Integer[] compilationEvs = new Integer[FortranWrapper.FORTRAN_F90_FILES.length + FortranWrapper.FORTRAN_F_FILES.length];
        int i = 0;
        MessagePrinter.printInfoMsg("Building fixed executables");
        for (String fortranFile : FortranWrapper.FORTRAN_F90_FILES) {
            String executable = NMMBEnvironment.FIX + fortranFile + FortranWrapper.SUFFIX_EXE;
            String src = NMMBEnvironment.FIX + fortranFile + FortranWrapper.SUFFIX_F90_SRC;

            compilationEvs[i++] = BINARY.fortranCompiler(FortranWrapper.MC_FLAG, FortranWrapper.SHARED_FLAG, FortranWrapper.CONVERT_PREFIX,
                    FortranWrapper.CONVERT_VALUE, FortranWrapper.TRACEBACK_FLAG, FortranWrapper.ASSUME_PREFIX, FortranWrapper.ASSUME_VALUE,
                    FortranWrapper.OPT_FLAG, FortranWrapper.FPMODEL_PREFIX, FortranWrapper.FPMODEL_VALUE, FortranWrapper.STACK_FLAG,
                    FortranWrapper.OFLAG, executable, src);
        }
        for (String fortranFile : FortranWrapper.FORTRAN_F_FILES) {
            String executable = NMMBEnvironment.FIX + fortranFile + FortranWrapper.SUFFIX_EXE;
            String src = NMMBEnvironment.FIX + fortranFile + FortranWrapper.SUFFIX_F_SRC;
            compilationEvs[i++] = BINARY.fortranCompiler(FortranWrapper.MC_FLAG, FortranWrapper.SHARED_FLAG, FortranWrapper.CONVERT_PREFIX,
                    FortranWrapper.CONVERT_VALUE, FortranWrapper.TRACEBACK_FLAG, FortranWrapper.ASSUME_PREFIX, FortranWrapper.ASSUME_VALUE,
                    FortranWrapper.OPT_FLAG, FortranWrapper.FPMODEL_PREFIX, FortranWrapper.FPMODEL_VALUE, FortranWrapper.STACK_FLAG,
                    FortranWrapper.OFLAG, executable, src);
        }
        // Sync master to wait for compilation
        for (i = 0; i < compilationEvs.length; ++i) {
            LOGGER_FIXED.debug("Compilation of " + i + " binary ended with status " + compilationEvs[i]);
            if (compilationEvs[i] != 0) {
                LOGGER_FIXED.error("[ERROR] Error compiling binary " + i);
                LOGGER_FIXED.error("Aborting...");
                System.exit(1);
            }
        }

        MessagePrinter.printInfoMsg("Finished building fixed executables");

        /* Begin binary calls ***********************************************************/
        MessagePrinter.printHeaderMsg("BEGIN");

        final int NUM_BINARIES = 16;
        Integer[] fixedBinariesEvs = new Integer[NUM_BINARIES];
        i = 0;

        MessagePrinter.printInfoMsg("Generate DEM height and sea mask files");
        String topoDir = NMMBEnvironment.GEODATA_DIR + "topo1kmDEM" + File.separator;
        String seamaskDEM = NMMBEnvironment.OUTPUT + "seamaskDEM";
        String heightDEM = NMMBEnvironment.OUTPUT + "heightDEM";
        fixedBinariesEvs[i++] = BINARY.smmount(topoDir, seamaskDEM, heightDEM);

        MessagePrinter.printInfoMsg("Generate landuse file");
        String landuseDataDir = NMMBEnvironment.GEODATA_DIR + "landuse_30s" + File.separator;;
        String landuse = NMMBEnvironment.OUTPUT + "landuse";
        String kount_landuse = NMMBEnvironment.OUTPUT + "kount_landuse";
        fixedBinariesEvs[i++] = BINARY.landuse(landuseDataDir, landuse, kount_landuse);

        MessagePrinter.printInfoMsg("Generate landusenew file");
        String landusenew = NMMBEnvironment.OUTPUT + "landusenew";
        String kount_landusenew = NMMBEnvironment.OUTPUT + "kount_landusenew";
        fixedBinariesEvs[i++] = BINARY.landusenew(NMMBEnvironment.GTOPO_DIR, landusenew, kount_landusenew);

        MessagePrinter.printInfoMsg("Generate mountains");
        String topo30sDir = NMMBEnvironment.GEODATA_DIR + "topo_30s" + File.separator;
        String heightmean = NMMBEnvironment.OUTPUT + "heightmean";
        fixedBinariesEvs[i++] = BINARY.topo(topo30sDir, heightmean);

        MessagePrinter.printInfoMsg("Generate standard deviation of topography height");
        String stdh = NMMBEnvironment.OUTPUT + "stdh";
        fixedBinariesEvs[i++] = BINARY.stdh(heightmean, seamaskDEM, topo30sDir, stdh);

        MessagePrinter.printInfoMsg("Generate envelope mountains");
        String height = NMMBEnvironment.OUTPUT + "height";
        fixedBinariesEvs[i++] = BINARY.envelope(heightmean, stdh, height);

        MessagePrinter.printInfoMsg("Generate top soil type file");
        String soiltypeDir = NMMBEnvironment.GEODATA_DIR + "soiltype_top_30s" + File.separator;
        String topsoiltype = NMMBEnvironment.OUTPUT + "topsoiltype";
        fixedBinariesEvs[i++] = BINARY.topsoiltype(seamaskDEM, soiltypeDir, topsoiltype);

        MessagePrinter.printInfoMsg("Generate bottom soil type file");
        String soiltypePath = NMMBEnvironment.GEODATA_DIR + "soiltype_bot_30s" + File.separator;
        String botsoiltype = NMMBEnvironment.OUTPUT + "botsoiltype";
        fixedBinariesEvs[i++] = BINARY.botsoiltype(seamaskDEM, soiltypePath, botsoiltype);

        MessagePrinter.printInfoMsg("Generate sea mask and reprocess mountains");
        String seamask = NMMBEnvironment.OUTPUT + "seamask";
        fixedBinariesEvs[i++] = BINARY.toposeamask(seamaskDEM, seamask, height, landuse, topsoiltype, botsoiltype);

        MessagePrinter.printInfoMsg("Reprocess standard deviation of topography height");
        fixedBinariesEvs[i++] = BINARY.stdhtopo(seamask, stdh);

        MessagePrinter.printInfoMsg("Generate deep soil temperature");
        String soiltempPath = NMMBEnvironment.GEODATA_DIR + "soiltemp_1deg" + File.separator;
        String deeptemperature = NMMBEnvironment.OUTPUT + "deeptemperature";
        fixedBinariesEvs[i++] = BINARY.deeptemperature(seamask, soiltempPath, deeptemperature);

        MessagePrinter.printInfoMsg("Generate maximum snow albedo");
        String maxsnowalbDir = NMMBEnvironment.GEODATA_DIR + "maxsnowalb" + File.separator;
        String snowalbedo = NMMBEnvironment.OUTPUT + "snowalbedo";
        fixedBinariesEvs[i++] = BINARY.snowalbedo(maxsnowalbDir, snowalbedo);

        MessagePrinter.printInfoMsg("Generate vertical coordinate");
        String dsg = NMMBEnvironment.OUTPUT + "dsg";
        fixedBinariesEvs[i++] = BINARY.vcgenerator(dsg);

        MessagePrinter.printInfoMsg("Generate highres roughness length for africa and asia");
        String roughnessDir = NMMBEnvironment.GEODATA_DIR + "roughness_025s" + File.separator;
        String roughness = NMMBEnvironment.OUTPUT + "roughness";
        fixedBinariesEvs[i++] = BINARY.roughness(roughnessDir, roughness);

        MessagePrinter.printInfoMsg("Generate co2 files");
        String co2_data_dir = NMMBEnvironment.GEODATA_DIR + "co2data" + File.separator;
        String co2_trans = NMMBEnvironment.OUTPUT + "co2_trans";
        fixedBinariesEvs[i++] = BINARY.gfdlco2(dsg, co2_data_dir, co2_trans);

        MessagePrinter.printInfoMsg("Generate lookup tables for aerosol scavenging collection efficiencies");
        String lookup_aerosol2_rh00 = NMMBEnvironment.OUTPUT + "lookup_aerosol2.dat.rh00";
        String lookup_aerosol2_rh50 = NMMBEnvironment.OUTPUT + "lookup_aerosol2.dat.rh50";
        String lookup_aerosol2_rh70 = NMMBEnvironment.OUTPUT + "lookup_aerosol2.dat.rh70";
        String lookup_aerosol2_rh80 = NMMBEnvironment.OUTPUT + "lookup_aerosol2.dat.rh80";
        String lookup_aerosol2_rh90 = NMMBEnvironment.OUTPUT + "lookup_aerosol2.dat.rh90";
        String lookup_aerosol2_rh95 = NMMBEnvironment.OUTPUT + "lookup_aerosol2.dat.rh95";
        String lookup_aerosol2_rh99 = NMMBEnvironment.OUTPUT + "lookup_aerosol2.dat.rh99";
        fixedBinariesEvs[i++] = BINARY.run_aerosol(lookup_aerosol2_rh00, lookup_aerosol2_rh50, lookup_aerosol2_rh70, lookup_aerosol2_rh80,
                lookup_aerosol2_rh90, lookup_aerosol2_rh95, lookup_aerosol2_rh99);

        /* Wait for binaries completion and check exit value *****************************/
        for (i = 0; i < fixedBinariesEvs.length; ++i) {
            LOGGER_FIXED.debug("Execution of " + i + " binary ended with status " + fixedBinariesEvs[i]);
            if (fixedBinariesEvs[i] != 0) {
                LOGGER_FIXED.error("[ERROR] Error executing binary " + i);
                LOGGER_FIXED.error("Aborting...");
                System.exit(1);
            }
        }

        /* Clean Up binaries ************************************************************/
        MessagePrinter.printInfoMsg("Clean up executables");
        for (String fortranFile : FortranWrapper.FORTRAN_F90_FILES) {
            String executable = NMMBEnvironment.FIX + fortranFile + FortranWrapper.SUFFIX_EXE;
            File f = new File(executable);
            if (f.exists()) {
                f.delete();
            }
        }
        for (String fortranFile : FortranWrapper.FORTRAN_F_FILES) {
            String executable = NMMBEnvironment.FIX + fortranFile + FortranWrapper.SUFFIX_EXE;
            File f = new File(executable);
            if (f.exists()) {
                f.delete();
            }
        }

        /* End binary calls *************************************************************/
        MessagePrinter.printHeaderMsg("END");

        LOGGER_FIXED.info("Fixed process finished");
    }

    /**
     * Performs the VARIABLE step
     * 
     */
    private static void doVariable(NMMBParameters nmmbParams) {
        LOGGER_VARIABLE.info("Enter variable process");
        LOGGER_VARIABLE.info("Variable process finished");
    }

    /**
     * Performs the UMO Model simulation step
     * 
     */
    private static void doUMOModel(NMMBParameters nmmbParams) {
        LOGGER_UMO_MODEL.info("Enter UMO Model process");
        LOGGER_UMO_MODEL.info("UMO Model process finished");
    }

    /**
     * Performs the POST step
     * 
     */
    private static void doPost(NMMBParameters nmmbParams) {
        LOGGER_POST.info("Enter post process");
        LOGGER_POST.info("Post process finished");
    }

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
        NMMBConfigManager nmmbConfigManager = null;
        try {
            LOGGER_MAIN.info("Loading NMMB Configuration file " + configurationFile);
            nmmbConfigManager = new NMMBConfigManager(configurationFile);
            LOGGER_MAIN.info("Configuration file loaded");
        } catch (ConfigurationException ce) {
            LOGGER_MAIN.error("[ERROR] Cannot load configuration file: " + configurationFile, ce);
            LOGGER_MAIN.error("Aborting...");
            System.exit(1);
        }

        // Compute the execution variables
        NMMBParameters nmmbParams = new NMMBParameters(nmmbConfigManager);

        // Prepare the execution
        nmmbParams.prepareExecution();

        // Fixed process (do before main time looping)
        if (nmmbParams.DO_FIXED) {
            doFixed(nmmbParams);
        }

        // Start main time loop
        Date currentDate = nmmbParams.START_DATE;
        while (!currentDate.after(nmmbParams.END_DATE)) {
            String currentDateSTR = NMMBConstants.STR_TO_DATE.format(currentDate);
            LOGGER_MAIN.info(currentDateSTR + " simulation started");

            // Define model output folder by case and date
            // FOLDER_OUTPUT_CASE = OUTNMMB + File.separator + CASE;
            // FOLDER_OUTPUT = OUTNMMB + File.separator + CASE + File.separator + STR_TO_DATE.format(CURRENT_DATE) +
            // HOUR;

            // Vrbl process
            if (nmmbParams.DO_VRBL) {
                doVariable(nmmbParams);
            }

            // UMO model run
            if (nmmbParams.DO_UMO) {
                doUMOModel(nmmbParams);
            }

            // Post process
            if (nmmbParams.DO_POST) {
                doPost(nmmbParams);
            }

            LOGGER_MAIN.info(currentDateSTR + " simulation finished");

            // Getting next simulation day
            currentDate = Date.from(currentDate.toInstant().plusSeconds(NMMBConstants.ONE_DAY_IN_SECONDS));
        }
    }
}
