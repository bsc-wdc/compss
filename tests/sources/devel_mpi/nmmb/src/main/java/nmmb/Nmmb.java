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
import nmmb.loggers.LoggerNames;
import nmmb.utils.FortranWrapper;
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
        Integer[] compilationEvs = new Integer[FortranWrapper.FIXED_FORTRAN_F90_FILES.length + FortranWrapper.FIXED_FORTRAN_F_FILES.length];
        int i = 0;
        MessagePrinter.printInfoMsg("Building fixed executables");
        for (String fortranFile : FortranWrapper.FIXED_FORTRAN_F90_FILES) {
            String executable = NMMBEnvironment.FIX + fortranFile + FortranWrapper.SUFFIX_EXE;
            String src = NMMBEnvironment.FIX + fortranFile + FortranWrapper.SUFFIX_F90_SRC;

            compilationEvs[i++] = BINARY.fortranCompiler(FortranWrapper.MC_FLAG, FortranWrapper.SHARED_FLAG, FortranWrapper.CONVERT_PREFIX,
                    FortranWrapper.CONVERT_VALUE, FortranWrapper.TRACEBACK_FLAG, FortranWrapper.ASSUME_PREFIX, FortranWrapper.ASSUME_VALUE,
                    FortranWrapper.OPT_FLAG, FortranWrapper.FPMODEL_PREFIX, FortranWrapper.FPMODEL_VALUE, FortranWrapper.STACK_FLAG,
                    FortranWrapper.OFLAG, executable, src);
        }
        for (String fortranFile : FortranWrapper.FIXED_FORTRAN_F_FILES) {
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
        String landuseDataDir = NMMBEnvironment.GEODATA_DIR + "landuse_30s" + File.separator;
        ;
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
        for (String fortranFile : FortranWrapper.FIXED_FORTRAN_F90_FILES) {
            String executable = NMMBEnvironment.FIX + fortranFile + FortranWrapper.SUFFIX_EXE;
            File f = new File(executable);
            if (f.exists()) {
                f.delete();
            }
        }
        for (String fortranFile : FortranWrapper.FIXED_FORTRAN_F_FILES) {
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

    /*
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ******************** VARIABLE STEP ****************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     * ***************************************************************************************************
     */
    private static void doVariable(NMMBParameters nmmbParams, Date currentDate, String folderOutputCase) {
        LOGGER_VARIABLE.info("Enter variable process");

        /* Prepare execution **************************************************************/
        nmmbParams.prepareVariableExecution(currentDate);

        /* Build the fortran executables *************************************************/
        Integer[] compilationEvs = new Integer[FortranWrapper.VARIABLE_FORTRAN_F90_FILES.length
                + FortranWrapper.VARIABLE_FORTRAN_F_FILES.length];
        int i = 0;
        LOGGER_VARIABLE.info("Building fixed executables");
        for (String fortranFile : FortranWrapper.VARIABLE_FORTRAN_F90_FILES) {
            String executable = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_EXE;
            String src = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_F90_SRC;

            compilationEvs[i++] = BINARY.fortranCompiler(FortranWrapper.MC_FLAG, FortranWrapper.SHARED_FLAG, FortranWrapper.CONVERT_PREFIX,
                    FortranWrapper.CONVERT_VALUE, FortranWrapper.TRACEBACK_FLAG, FortranWrapper.ASSUME_PREFIX, FortranWrapper.ASSUME_VALUE,
                    FortranWrapper.OPT_FLAG, FortranWrapper.FPMODEL_PREFIX, FortranWrapper.FPMODEL_VALUE, FortranWrapper.STACK_FLAG,
                    FortranWrapper.OFLAG, executable, src);
        }
        for (String fortranFile : FortranWrapper.VARIABLE_FORTRAN_F_FILES) {
            String executable = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_EXE;
            String src = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_F_SRC;
            compilationEvs[i++] = BINARY.fortranCompiler(FortranWrapper.MC_FLAG, FortranWrapper.SHARED_FLAG, FortranWrapper.CONVERT_PREFIX,
                    FortranWrapper.CONVERT_VALUE, FortranWrapper.TRACEBACK_FLAG, FortranWrapper.ASSUME_PREFIX, FortranWrapper.ASSUME_VALUE,
                    FortranWrapper.OPT_FLAG, FortranWrapper.FPMODEL_PREFIX, FortranWrapper.FPMODEL_VALUE, FortranWrapper.STACK_FLAG,
                    FortranWrapper.OFLAG, executable, src);
        }
        // Sync master to wait for compilation
        for (i = 0; i < compilationEvs.length; ++i) {
            LOGGER_VARIABLE.debug("Compilation of " + i + " binary ended with status " + compilationEvs[i]);
            if (compilationEvs[i] != 0) {
                LOGGER_VARIABLE.error("[ERROR] Error compiling binary " + i);
                LOGGER_VARIABLE.error("Aborting...");
                System.exit(1);
            }
        }

        MessagePrinter.printInfoMsg("Finished building fixed executables");

        /* Begin binary calls ***********************************************************/
        final int NUM_BINARIES = 12;
        Integer[] variableBinariesEvs = new Integer[NUM_BINARIES];
        i = 0;

        LOGGER_VARIABLE.info("degrib gfs global data");
        String CW = NMMBEnvironment.OUTPUT + "00_CW.dump";
        String ICEC = NMMBEnvironment.OUTPUT + "00_ICEC.dump";
        String SH = NMMBEnvironment.OUTPUT + "00_SH.dump";
        String SOILT2 = NMMBEnvironment.OUTPUT + "00_SOILT2.dump";
        String SOILT4 = NMMBEnvironment.OUTPUT + "00_SOILT4.dump";
        String SOILW2 = NMMBEnvironment.OUTPUT + "00_SOILW2.dump";
        String SOILW4 = NMMBEnvironment.OUTPUT + "00_SOILW4.dump";
        String TT = NMMBEnvironment.OUTPUT + "00_TT.dump";
        String VV = NMMBEnvironment.OUTPUT + "00_VV.dump";
        String HH = NMMBEnvironment.OUTPUT + "00_HH.dump";
        String PRMSL = NMMBEnvironment.OUTPUT + "00_PRMSL.dump";
        String SOILT1 = NMMBEnvironment.OUTPUT + "00_SOILT1.dump";
        String SOILT3 = NMMBEnvironment.OUTPUT + "00_SOILT3.dump";
        String SOILW1 = NMMBEnvironment.OUTPUT + "00_SOILW1.dump";
        String SOILW3 = NMMBEnvironment.OUTPUT + "00_SOILW3.dump";
        String SST_TS = NMMBEnvironment.OUTPUT + "00_SST_TS.dump";
        String UU = NMMBEnvironment.OUTPUT + "00_UU.dump";
        String WEASD = NMMBEnvironment.OUTPUT + "00_WEASD.dump";
        variableBinariesEvs[i++] = BINARY.degribgfs_generic_05(CW, ICEC, SH, SOILT2, SOILT4, SOILW2, SOILW4, TT, VV, HH, PRMSL, SOILT1,
                SOILT3, SOILW1, SOILW3, SST_TS, UU, WEASD);

        LOGGER_VARIABLE.info("GFS 2 Model");
        String GFS_file = NMMBEnvironment.OUTPUT + "131140000.gfs";
        variableBinariesEvs[i++] = BINARY.gfs2model_rrtm(CW, ICEC, SH, SOILT2, SOILT4, SOILW2, SOILW4, TT, VV, HH, PRMSL, SOILT1, SOILT3,
                SOILW1, SOILW3, SST_TS, UU, WEASD, GFS_file);

        LOGGER_VARIABLE.info("INC RRTM");
        String deco = NMMBEnvironment.VRB_INCLUDE_DIR + "deco.inc";
        variableBinariesEvs[i++] = BINARY.inc_rrtm(GFS_file, deco);

        LOGGER_VARIABLE.info("CNV RRTM");
        String llspl000 = NMMBEnvironment.OUTPUT + "llspl.000";
        String outtmp = NMMBEnvironment.OUTPUT + "llstmp";
        String outmst = NMMBEnvironment.OUTPUT + "llsmst";
        String outsst = NMMBEnvironment.OUTPUT + "llgsst";
        String outsno = NMMBEnvironment.OUTPUT + "llgsno";
        String outcic = NMMBEnvironment.OUTPUT + "llgcic";
        variableBinariesEvs[i++] = BINARY.cnv_rrtm(GFS_file, llspl000, outtmp, outmst, outsst, outsno, outcic, deco);

        LOGGER_VARIABLE.info("Degrib 0.5 deg sst");
        String llgsst05 = NMMBEnvironment.OUTPUT + "llgsst05";
        variableBinariesEvs[i++] = BINARY.degribsst(llgsst05);

        LOGGER_VARIABLE.info("Prepare climatological albedo");
        String seamask = NMMBEnvironment.OUTPUT + "seamask";
        String albedo = NMMBEnvironment.OUTPUT + "albedo";
        String albedobase = NMMBEnvironment.OUTPUT + "albedobase";
        String albedomnth = NMMBEnvironment.GEODATA_DIR + "albedo" + File.separator + "albedomnth";
        variableBinariesEvs[i++] = BINARY.albedo(llspl000, seamask, albedo, albedobase, albedomnth);

        LOGGER_VARIABLE.info("Prepare rrtm climatological albedos");
        String albedorrtm = NMMBEnvironment.OUTPUT + "albedorrtm";
        String albedorrtm1degDir = NMMBEnvironment.GEODATA_DIR + "albedo_rrtm1deg" + File.separator;
        variableBinariesEvs[i++] = BINARY.albedorrtm(llspl000, seamask, albedorrtm, albedorrtm1degDir);

        LOGGER_VARIABLE.info("Prepare climatological vegetation fraction");
        String vegfrac = NMMBEnvironment.OUTPUT + "vegfrac";
        variableBinariesEvs[i++] = BINARY.vegfrac(llspl000, seamask, vegfrac);

        LOGGER_VARIABLE.info("Prepare z0 and initial ustar");
        String landuse = NMMBEnvironment.OUTPUT + "landuse";
        String topsoiltype = NMMBEnvironment.OUTPUT + "topsoiltype";
        String height = NMMBEnvironment.OUTPUT + "height";
        String stdh = NMMBEnvironment.OUTPUT + "stdh";
        String z0base = NMMBEnvironment.OUTPUT + "z0base";
        String z0 = NMMBEnvironment.OUTPUT + "z0";
        String ustar = NMMBEnvironment.OUTPUT + "ustar";
        variableBinariesEvs[i++] = BINARY.z0vegfrac(seamask, landuse, topsoiltype, height, stdh, vegfrac, z0base, z0, ustar);

        LOGGER_VARIABLE.info("Interpolate to model grid and execute allprep (fcst)");
        String sst05 = NMMBEnvironment.OUTPUT + "sst05";
        String deeptemperature = NMMBEnvironment.OUTPUT + "deeptemperature";
        String snowalbedo = NMMBEnvironment.OUTPUT + "snowalbedo";
        String landusenew = NMMBEnvironment.OUTPUT + "landusenew";
        String llgsst = NMMBEnvironment.OUTPUT + "llgsst";
        String llgsno = NMMBEnvironment.OUTPUT + "llgsno";
        String llgcic = NMMBEnvironment.OUTPUT + "llgcic";
        String llsmst = NMMBEnvironment.OUTPUT + "llsmst";
        String llstmp = NMMBEnvironment.OUTPUT + "llstmp";
        String albedorrtmcorr = NMMBEnvironment.OUTPUT + "albedorrtmcorr";
        String dzsoil = NMMBEnvironment.OUTPUT + "dzsoil";
        String tskin = NMMBEnvironment.OUTPUT + "tskin";
        String sst = NMMBEnvironment.OUTPUT + "sst";
        String snow = NMMBEnvironment.OUTPUT + "snow";
        String snowheight = NMMBEnvironment.OUTPUT + "snowheight";
        String cice = NMMBEnvironment.OUTPUT + "cice";
        String seamaskcorr = NMMBEnvironment.OUTPUT + "seamaskcorr";
        String landusecorr = NMMBEnvironment.OUTPUT + "landusecorr";
        String landusenewcorr = NMMBEnvironment.OUTPUT + "landusenewcorr";
        String topsoiltypecorr = NMMBEnvironment.OUTPUT + "topsoiltypecorr";
        String vegfraccorr = NMMBEnvironment.OUTPUT + "vegfraccorr";
        String z0corr = NMMBEnvironment.OUTPUT + "z0corr";
        String z0basecorr = NMMBEnvironment.OUTPUT + "z0basecorr";
        String emissivity = NMMBEnvironment.OUTPUT + "emissivity";
        String canopywater = NMMBEnvironment.OUTPUT + "canopywater";
        String frozenprecratio = NMMBEnvironment.OUTPUT + "frozenprecratio";
        String smst = NMMBEnvironment.OUTPUT + "smst";
        String sh2o = NMMBEnvironment.OUTPUT + "sh2o";
        String stmp = NMMBEnvironment.OUTPUT + "stmp";
        String dsg = NMMBEnvironment.OUTPUT + "dsg";
        String fcst = NMMBEnvironment.OUTPUT + "fcst";
        String fcstDir = NMMBEnvironment.OUTPUT + "fcst";
        String bocoPrefix = NMMBEnvironment.OUTPUT + "boco.";
        String llsplPrefix = NMMBEnvironment.OUTPUT + "llspl.";
        variableBinariesEvs[i++] = BINARY.allprep(llspl000, llgsst05, sst05, height, seamask, stdh, deeptemperature, snowalbedo, z0, z0base,
                landuse, landusenew, topsoiltype, vegfrac, albedorrtm, llgsst, llgsno, llgcic, llsmst, llstmp, albedorrtmcorr, dzsoil,
                tskin, sst, snow, snowheight, cice, seamaskcorr, landusecorr, landusenewcorr, topsoiltypecorr, vegfraccorr, z0corr,
                z0basecorr, emissivity, canopywater, frozenprecratio, smst, sh2o, stmp, dsg, fcst, albedo, ustar, fcstDir, bocoPrefix,
                llsplPrefix);

        LOGGER_VARIABLE.info("Prepare the dust related variable (soildust)");
        String source = NMMBEnvironment.OUTPUT + "source";
        String sourceNETCDF = NMMBEnvironment.OUTPUT + "source.nc";
        variableBinariesEvs[i++] = BINARY.readpaulsource(seamask, source, sourceNETCDF);

        LOGGER_VARIABLE.info("Dust Start");
        String soildust = NMMBEnvironment.OUTPUT + "soildust";
        String kount_landuse = NMMBEnvironment.OUTPUT + "kount_landuse";
        String kount_landusenew = NMMBEnvironment.OUTPUT + "kount_landusenew";
        String roughness = NMMBEnvironment.OUTPUT + "roughness";
        variableBinariesEvs[i++] = BINARY.dust_start(llspl000, soildust, snow, topsoiltypecorr, landusecorr, landusenewcorr, kount_landuse,
                kount_landusenew, vegfrac, height, seamask, source, z0corr, roughness);

        /* Wait for binaries completion and check exit value *****************************/
        for (i = 0; i < variableBinariesEvs.length; ++i) {
            LOGGER_VARIABLE.debug("Execution of " + i + " binary ended with status " + variableBinariesEvs[i]);
            if (variableBinariesEvs[i] != 0) {
                LOGGER_VARIABLE.error("[ERROR] Error executing binary " + i);
                LOGGER_VARIABLE.error("Aborting...");
                System.exit(1);
            }
        }

        /* Clean Up binaries ************************************************************/
        LOGGER_VARIABLE.info("Clean up executables");
        for (String fortranFile : FortranWrapper.VARIABLE_FORTRAN_F90_FILES) {
            String executable = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_EXE;
            File f = new File(executable);
            if (f.exists()) {
                f.delete();
            }
        }
        for (String fortranFile : FortranWrapper.VARIABLE_FORTRAN_F_FILES) {
            String executable = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_EXE;
            File f = new File(executable);
            if (f.exists()) {
                f.delete();
            }
        }

        /* Post execution **************************************************************/
        nmmbParams.postVariableExecution(folderOutputCase);

        LOGGER_VARIABLE.info("Variable process finished");
    }

    /**
     * Performs the UMO Model simulation step
     * 
     */
    private static void doUMOModel(NMMBParameters nmmbParams, Date currentDate) {
        LOGGER_UMO_MODEL.info("Enter UMO Model process");
        LOGGER_UMO_MODEL.info("UMO Model process finished");
    }

    /**
     * Performs the POST step
     * 
     */
    private static void doPost(NMMBParameters nmmbParams, Date currentDate) {
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
            String hourSTR = (nmmbParams.HOUR < 10) ? "0" + String.valueOf(nmmbParams.HOUR) : String.valueOf(nmmbParams.HOUR);

            LOGGER_MAIN.info(currentDateSTR + " simulation started");

            // Define model output folder by case and date
            String folderOutputCase = NMMBEnvironment.OUTNMMB + nmmbParams.CASE + File.separator;
            String folderOutput = NMMBEnvironment.OUTNMMB + nmmbParams.CASE + File.separator + currentDateSTR + hourSTR;

            // Vrbl process
            if (nmmbParams.DO_VRBL) {
                doVariable(nmmbParams, currentDate, folderOutputCase);
            }

            // UMO model run
            if (nmmbParams.DO_UMO) {
                doUMOModel(nmmbParams, currentDate);
            }

            // Post process
            if (nmmbParams.DO_POST) {
                doPost(nmmbParams, currentDate);
            }

            LOGGER_MAIN.info(currentDateSTR + " simulation finished");

            // Getting next simulation day
            currentDate = Date.from(currentDate.toInstant().plusSeconds(NMMBConstants.ONE_DAY_IN_SECONDS));
        }
    }
}
