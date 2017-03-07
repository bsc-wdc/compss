package nmmb;

import java.io.File;
import java.util.Date;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import binary.BINARY;
import mpi.MPI;
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
        MessagePrinter fixedMP = new MessagePrinter(LOGGER_FIXED);

        /* Build the fortran executables *************************************************/
        Integer[] compilationEvs = new Integer[FortranWrapper.FIXED_FORTRAN_F90_FILES.length + FortranWrapper.FIXED_FORTRAN_F_FILES.length];
        int i = 0;
        fixedMP.printInfoMsg("Building fixed executables");
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

        fixedMP.printInfoMsg("Finished building fixed executables");

        /* Begin binary calls ***********************************************************/
        fixedMP.printHeaderMsg("BEGIN");

        final int NUM_BINARIES = 16;
        Integer[] fixedBinariesEvs = new Integer[NUM_BINARIES];
        i = 0;

        fixedMP.printInfoMsg("Generate DEM height and sea mask files");
        String topoDir = NMMBEnvironment.GEODATA_DIR + "topo1kmDEM" + File.separator;
        String seamaskDEM = NMMBEnvironment.OUTPUT + "seamaskDEM";
        String heightDEM = NMMBEnvironment.OUTPUT + "heightDEM";
        fixedBinariesEvs[i++] = BINARY.smmount(topoDir, seamaskDEM, heightDEM);

        fixedMP.printInfoMsg("Generate landuse file");
        String landuseDataDir = NMMBEnvironment.GEODATA_DIR + "landuse_30s" + File.separator;
        ;
        String landuse = NMMBEnvironment.OUTPUT + "landuse";
        String kount_landuse = NMMBEnvironment.OUTPUT + "kount_landuse";
        fixedBinariesEvs[i++] = BINARY.landuse(landuseDataDir, landuse, kount_landuse);

        fixedMP.printInfoMsg("Generate landusenew file");
        String landusenew = NMMBEnvironment.OUTPUT + "landusenew";
        String kount_landusenew = NMMBEnvironment.OUTPUT + "kount_landusenew";
        fixedBinariesEvs[i++] = BINARY.landusenew(NMMBEnvironment.GTOPO_DIR, landusenew, kount_landusenew);

        fixedMP.printInfoMsg("Generate mountains");
        String topo30sDir = NMMBEnvironment.GEODATA_DIR + "topo_30s" + File.separator;
        String heightmean = NMMBEnvironment.OUTPUT + "heightmean";
        fixedBinariesEvs[i++] = BINARY.topo(topo30sDir, heightmean);

        fixedMP.printInfoMsg("Generate standard deviation of topography height");
        String stdh = NMMBEnvironment.OUTPUT + "stdh";
        fixedBinariesEvs[i++] = BINARY.stdh(heightmean, seamaskDEM, topo30sDir, stdh);

        fixedMP.printInfoMsg("Generate envelope mountains");
        String height = NMMBEnvironment.OUTPUT + "height";
        fixedBinariesEvs[i++] = BINARY.envelope(heightmean, stdh, height);

        fixedMP.printInfoMsg("Generate top soil type file");
        String soiltypeDir = NMMBEnvironment.GEODATA_DIR + "soiltype_top_30s" + File.separator;
        String topsoiltype = NMMBEnvironment.OUTPUT + "topsoiltype";
        fixedBinariesEvs[i++] = BINARY.topsoiltype(seamaskDEM, soiltypeDir, topsoiltype);

        fixedMP.printInfoMsg("Generate bottom soil type file");
        String soiltypePath = NMMBEnvironment.GEODATA_DIR + "soiltype_bot_30s" + File.separator;
        String botsoiltype = NMMBEnvironment.OUTPUT + "botsoiltype";
        fixedBinariesEvs[i++] = BINARY.botsoiltype(seamaskDEM, soiltypePath, botsoiltype);

        fixedMP.printInfoMsg("Generate sea mask and reprocess mountains");
        String seamask = NMMBEnvironment.OUTPUT + "seamask";
        fixedBinariesEvs[i++] = BINARY.toposeamask(seamaskDEM, seamask, height, landuse, topsoiltype, botsoiltype);

        fixedMP.printInfoMsg("Reprocess standard deviation of topography height");
        fixedBinariesEvs[i++] = BINARY.stdhtopo(seamask, stdh);

        fixedMP.printInfoMsg("Generate deep soil temperature");
        String soiltempPath = NMMBEnvironment.GEODATA_DIR + "soiltemp_1deg" + File.separator;
        String deeptemperature = NMMBEnvironment.OUTPUT + "deeptemperature";
        fixedBinariesEvs[i++] = BINARY.deeptemperature(seamask, soiltempPath, deeptemperature);

        fixedMP.printInfoMsg("Generate maximum snow albedo");
        String maxsnowalbDir = NMMBEnvironment.GEODATA_DIR + "maxsnowalb" + File.separator;
        String snowalbedo = NMMBEnvironment.OUTPUT + "snowalbedo";
        fixedBinariesEvs[i++] = BINARY.snowalbedo(maxsnowalbDir, snowalbedo);

        fixedMP.printInfoMsg("Generate vertical coordinate");
        String dsg = NMMBEnvironment.OUTPUT + "dsg";
        fixedBinariesEvs[i++] = BINARY.vcgenerator(dsg);

        fixedMP.printInfoMsg("Generate highres roughness length for africa and asia");
        String roughnessDir = NMMBEnvironment.GEODATA_DIR + "roughness_025s" + File.separator;
        String roughness = NMMBEnvironment.OUTPUT + "roughness";
        fixedBinariesEvs[i++] = BINARY.roughness(roughnessDir, roughness);

        fixedMP.printInfoMsg("Generate co2 files");
        String co2_data_dir = NMMBEnvironment.GEODATA_DIR + "co2data" + File.separator;
        String co2_trans = NMMBEnvironment.OUTPUT + "co2_trans";
        fixedBinariesEvs[i++] = BINARY.gfdlco2(dsg, co2_data_dir, co2_trans);

        fixedMP.printInfoMsg("Generate lookup tables for aerosol scavenging collection efficiencies");
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
        fixedMP.printInfoMsg("Clean up executables");
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

        /* End *************************************************************************/
        fixedMP.printHeaderMsg("END");

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

    private static void compileVariable() {
        /* Build the fortran objects *************************************************/
        Integer[] depCompilationEvs = new Integer[FortranWrapper.VARIABLE_FORTRAN_F90_DEP_FILES.length];
        int objectIndex = 0;
        for (String fortranFile : FortranWrapper.VARIABLE_FORTRAN_F90_DEP_FILES) {
            String moduleDir = NMMBEnvironment.VRB;
            String object = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_OBJECT;
            String src = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_F90_SRC;

            depCompilationEvs[objectIndex++] = BINARY.fortranCompileObject(FortranWrapper.MC_FLAG, FortranWrapper.SHARED_FLAG,
                    FortranWrapper.CONVERT_PREFIX, FortranWrapper.CONVERT_VALUE, FortranWrapper.TRACEBACK_FLAG,
                    FortranWrapper.ASSUME_PREFIX, FortranWrapper.ASSUME_VALUE, FortranWrapper.OPT_FLAG, FortranWrapper.FPMODEL_PREFIX,
                    FortranWrapper.FPMODEL_VALUE, FortranWrapper.STACK_FLAG, FortranWrapper.CFLAG, src, FortranWrapper.OFLAG, object,
                    FortranWrapper.MODULE_FLAG, moduleDir);
        }
        // Sync to check compilation status (dependency with task object is also respected if this sync is erased)
        for (int i = 0; i < depCompilationEvs.length; ++i) {
            LOGGER_VARIABLE.debug("Compilation of " + i + " dependant binary ended with status " + depCompilationEvs[i]);
            if (depCompilationEvs[i] != 0) {
                LOGGER_VARIABLE.error("[ERROR] Error compiling binary " + i);
                LOGGER_VARIABLE.error("Aborting...");
                System.exit(1);
            }
        }

        /* Build the fortran executables *************************************************/
        Integer[] compilationEvs = new Integer[FortranWrapper.VARIABLE_FORTRAN_F90_FILES.length
                + FortranWrapper.VARIABLE_FORTRAN_F_FILES.length + FortranWrapper.VARIABLE_FORTRAN_F_FILES_WITH_W3.length
                + FortranWrapper.VARIABLE_FORTRAN_F_FILES_WITH_DEPS.length];

        int executableIndex = 0;
        for (String fortranFile : FortranWrapper.VARIABLE_FORTRAN_F90_FILES) {
            String executable = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_EXE;
            String src = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_F90_SRC;

            compilationEvs[executableIndex++] = BINARY.fortranCompiler(FortranWrapper.MC_FLAG, FortranWrapper.SHARED_FLAG,
                    FortranWrapper.CONVERT_PREFIX, FortranWrapper.CONVERT_VALUE, FortranWrapper.TRACEBACK_FLAG,
                    FortranWrapper.ASSUME_PREFIX, FortranWrapper.ASSUME_VALUE, FortranWrapper.OPT_FLAG, FortranWrapper.FPMODEL_PREFIX,
                    FortranWrapper.FPMODEL_VALUE, FortranWrapper.STACK_FLAG, FortranWrapper.OFLAG, executable, src);
        }
        for (String fortranFile : FortranWrapper.VARIABLE_FORTRAN_F_FILES) {
            String executable = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_EXE;
            String src = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_F_SRC;
            compilationEvs[executableIndex++] = BINARY.fortranCompiler(FortranWrapper.MC_FLAG, FortranWrapper.SHARED_FLAG,
                    FortranWrapper.CONVERT_PREFIX, FortranWrapper.CONVERT_VALUE, FortranWrapper.TRACEBACK_FLAG,
                    FortranWrapper.ASSUME_PREFIX, FortranWrapper.ASSUME_VALUE, FortranWrapper.OPT_FLAG, FortranWrapper.FPMODEL_PREFIX,
                    FortranWrapper.FPMODEL_VALUE, FortranWrapper.STACK_FLAG, FortranWrapper.OFLAG, executable, src);
        }
        for (String fortranFile : FortranWrapper.VARIABLE_FORTRAN_F_FILES_WITH_W3) {
            String executable = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_EXE;
            String src = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_F_SRC;
            String w3LibFlag = "-L" + NMMBEnvironment.UMO_LIBS + FortranWrapper.W3_LIB_DIR;
            String bacioLibFlag = "-L" + NMMBEnvironment.UMO_LIBS + FortranWrapper.BACIO_LIB_DIR;
            compilationEvs[executableIndex++] = BINARY.fortranCompilerWithW3(FortranWrapper.MC_FLAG, FortranWrapper.SHARED_FLAG,
                    FortranWrapper.CONVERT_PREFIX, FortranWrapper.CONVERT_VALUE, FortranWrapper.TRACEBACK_FLAG,
                    FortranWrapper.ASSUME_PREFIX, FortranWrapper.ASSUME_VALUE, FortranWrapper.OPT_FLAG, FortranWrapper.FPMODEL_PREFIX,
                    FortranWrapper.FPMODEL_VALUE, FortranWrapper.STACK_FLAG, FortranWrapper.OFLAG, executable, src, w3LibFlag, bacioLibFlag,
                    FortranWrapper.W3_FLAG, FortranWrapper.BACIO_FLAG);
        }
        for (String fortranFile : FortranWrapper.VARIABLE_FORTRAN_F_FILES_WITH_DEPS) {
            String executable = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_EXE;
            String src = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_F_SRC;
            String object = NMMBEnvironment.VRB + FortranWrapper.MODULE_FLT + FortranWrapper.SUFFIX_OBJECT;
            compilationEvs[executableIndex++] = BINARY.fortranCompileWithObject(FortranWrapper.MC_FLAG, FortranWrapper.SHARED_FLAG,
                    FortranWrapper.CONVERT_PREFIX, FortranWrapper.CONVERT_VALUE, FortranWrapper.TRACEBACK_FLAG,
                    FortranWrapper.ASSUME_PREFIX, FortranWrapper.ASSUME_VALUE, FortranWrapper.OPT_FLAG, FortranWrapper.FPMODEL_PREFIX,
                    FortranWrapper.FPMODEL_VALUE, FortranWrapper.STACK_FLAG, FortranWrapper.OFLAG, executable, src, object);
        }
        // Sync master to wait for compilation
        for (int i = 0; i < compilationEvs.length; ++i) {
            LOGGER_VARIABLE.debug("Compilation of " + i + " binary ended with status " + compilationEvs[i]);
            if (compilationEvs[i] != 0) {
                LOGGER_VARIABLE.error("[ERROR] Error compiling binary " + i);
                LOGGER_VARIABLE.error("Aborting...");
                System.exit(1);
            }
        }
    }

    private static void cleanUpVariableExe() {
        for (String fortranFile : FortranWrapper.VARIABLE_FORTRAN_F90_DEP_FILES) {
            String executable = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_EXE;
            File f = new File(executable);
            if (f.exists()) {
                f.delete();
            }
        }
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
        for (String fortranFile : FortranWrapper.VARIABLE_FORTRAN_F_FILES_WITH_W3) {
            String executable = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_EXE;
            File f = new File(executable);
            if (f.exists()) {
                f.delete();
            }
        }
        for (String fortranFile : FortranWrapper.VARIABLE_FORTRAN_F_FILES_WITH_DEPS) {
            String executable = NMMBEnvironment.VRB + fortranFile + FortranWrapper.SUFFIX_EXE;
            File f = new File(executable);
            if (f.exists()) {
                f.delete();
            }
        }
    }

    private static void doVariable(NMMBParameters nmmbParams, Date currentDate) {
        LOGGER_VARIABLE.info("Enter variable process");

        /* Prepare execution **************************************************************/
        nmmbParams.prepareVariableExecution(currentDate);
        MessagePrinter variableMP = new MessagePrinter(LOGGER_VARIABLE);

        /* Compile ************************************************************************/
        variableMP.printInfoMsg("Building variable executables");
        compileVariable();
        variableMP.printInfoMsg("Finished building variable executables");

        /* Set variables for binary calls *************************************************/
        variableMP.printHeaderMsg("BEGIN");

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

        String GFS_file = NMMBEnvironment.OUTPUT + "131140000.gfs";

        String deco = NMMBEnvironment.VRB;

        String llspl000 = NMMBEnvironment.OUTPUT + "llspl.000";
        String outtmp = NMMBEnvironment.OUTPUT + "llstmp";
        String outmst = NMMBEnvironment.OUTPUT + "llsmst";
        String outsst = NMMBEnvironment.OUTPUT + "llgsst";
        String outsno = NMMBEnvironment.OUTPUT + "llgsno";
        String outcic = NMMBEnvironment.OUTPUT + "llgcic";

        String llgsst05 = NMMBEnvironment.OUTPUT + "llgsst05";
        String sstfileinPath = NMMBEnvironment.OUTPUT + "sst2dvar_grb_0.5";

        String seamask = NMMBEnvironment.OUTPUT + "seamask";
        String albedo = NMMBEnvironment.OUTPUT + "albedo";
        String albedobase = NMMBEnvironment.OUTPUT + "albedobase";
        String albedomnth = NMMBEnvironment.GEODATA_DIR + "albedo" + File.separator + "albedomnth";

        String albedorrtm = NMMBEnvironment.OUTPUT + "albedorrtm";
        String albedorrtm1degDir = NMMBEnvironment.GEODATA_DIR + "albedo_rrtm1deg" + File.separator;

        String vegfrac = NMMBEnvironment.OUTPUT + "vegfrac";
        String vegfracmnth = NMMBEnvironment.GEODATA_DIR + "vegfrac" + File.separator + "vegfracmnth";

        String landuse = NMMBEnvironment.OUTPUT + "landuse";
        String topsoiltype = NMMBEnvironment.OUTPUT + "topsoiltype";
        String height = NMMBEnvironment.OUTPUT + "height";
        String stdh = NMMBEnvironment.OUTPUT + "stdh";
        String z0base = NMMBEnvironment.OUTPUT + "z0base";
        String z0 = NMMBEnvironment.OUTPUT + "z0";
        String ustar = NMMBEnvironment.OUTPUT + "ustar";

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

        String source = NMMBEnvironment.OUTPUT + "source";
        String sourceNETCDF = NMMBEnvironment.OUTPUT + "source.nc";
        String sourceNCIncludeDir = NMMBEnvironment.VRB_INCLUDE_DIR;

        String soildust = NMMBEnvironment.OUTPUT + "soildust";
        String kount_landuse = NMMBEnvironment.OUTPUT + "kount_landuse";
        String kount_landusenew = NMMBEnvironment.OUTPUT + "kount_landusenew";
        String roughness = NMMBEnvironment.OUTPUT + "roughness";

        /* Begin binary calls ***********************************************************/
        final int NUM_BINARIES = 12;
        Integer[] variableBinariesEvs = new Integer[NUM_BINARIES];
        int binaryIndex = 0;

        variableMP.printInfoMsg("degrib gfs global data");
        variableBinariesEvs[binaryIndex++] = BINARY.degribgfs_generic_05(CW, ICEC, SH, SOILT2, SOILT4, SOILW2, SOILW4, TT, VV, HH, PRMSL,
                SOILT1, SOILT3, SOILW1, SOILW3, SST_TS, UU, WEASD);

        variableMP.printInfoMsg("GFS 2 Model");
        variableBinariesEvs[binaryIndex++] = BINARY.gfs2model_rrtm(CW, ICEC, SH, SOILT2, SOILT4, SOILW2, SOILW4, TT, VV, HH, PRMSL, SOILT1,
                SOILT3, SOILW1, SOILW3, SST_TS, UU, WEASD, GFS_file);

        variableMP.printInfoMsg("INC RRTM");
        variableBinariesEvs[binaryIndex++] = BINARY.inc_rrtm(GFS_file, deco);

        variableMP.printInfoMsg("CNV RRTM");
        variableBinariesEvs[binaryIndex++] = BINARY.cnv_rrtm(GFS_file, llspl000, outtmp, outmst, outsst, outsno, outcic);

        variableMP.printInfoMsg("Degrib 0.5 deg sst");
        variableBinariesEvs[binaryIndex++] = BINARY.degribsst(llgsst05, sstfileinPath);

        variableMP.printInfoMsg("Prepare climatological albedo");
        variableBinariesEvs[binaryIndex++] = BINARY.albedo(llspl000, seamask, albedo, albedobase, albedomnth);

        variableMP.printInfoMsg("Prepare rrtm climatological albedos");
        variableBinariesEvs[binaryIndex++] = BINARY.albedorrtm(llspl000, seamask, albedorrtm, albedorrtm1degDir);

        variableMP.printInfoMsg("Prepare climatological vegetation fraction");
        variableBinariesEvs[binaryIndex++] = BINARY.vegfrac(llspl000, seamask, vegfrac, vegfracmnth);

        variableMP.printInfoMsg("Prepare z0 and initial ustar");
        variableBinariesEvs[binaryIndex++] = BINARY.z0vegfrac(seamask, landuse, topsoiltype, height, stdh, vegfrac, z0base, z0, ustar);

        variableMP.printInfoMsg("Interpolate to model grid and execute allprep (fcst)");
        variableBinariesEvs[binaryIndex++] = BINARY.allprep(llspl000, llgsst05, sst05, height, seamask, stdh, deeptemperature, snowalbedo,
                z0, z0base, landuse, landusenew, topsoiltype, vegfrac, albedorrtm, llgsst, llgsno, llgcic, llsmst, llstmp, albedorrtmcorr,
                dzsoil, tskin, sst, snow, snowheight, cice, seamaskcorr, landusecorr, landusenewcorr, topsoiltypecorr, vegfraccorr, z0corr,
                z0basecorr, emissivity, canopywater, frozenprecratio, smst, sh2o, stmp, dsg, fcst, albedo, ustar, fcstDir, bocoPrefix,
                llsplPrefix);

        variableMP.printInfoMsg("Prepare the dust related variable (soildust)");
        variableBinariesEvs[binaryIndex++] = BINARY.readpaulsource(seamask, source, sourceNETCDF, sourceNCIncludeDir);

        variableMP.printInfoMsg("Dust Start");
        variableBinariesEvs[binaryIndex++] = BINARY.dust_start(llspl000, soildust, snow, topsoiltypecorr, landusecorr, landusenewcorr,
                kount_landuse, kount_landusenew, vegfrac, height, seamask, source, z0corr, roughness);

        /* Wait for binaries completion and check exit value *****************************/
        for (int i = 0; i < variableBinariesEvs.length; ++i) {
            LOGGER_VARIABLE.debug("Execution of " + i + " binary ended with status " + variableBinariesEvs[i]);
            if (variableBinariesEvs[i] != 0) {
                LOGGER_VARIABLE.error("[ERROR] Error executing binary " + i);
                LOGGER_VARIABLE.error("Aborting...");
                System.exit(1);
            }
        }

        variableMP.printHeaderMsg("END");

        /* Clean Up binaries ************************************************************/
        variableMP.printInfoMsg("Clean up executables");
        cleanUpVariableExe();

        /* Post execution **************************************************************/
        String folderOutputCase = NMMBEnvironment.OUTNMMB + nmmbParams.CASE + File.separator;
        nmmbParams.postVariableExecution(folderOutputCase);

        LOGGER_VARIABLE.info("Variable process finished");
    }

    /**
     * Performs the UMO Model simulation step
     * 
     */
    private static void doUMOModel(NMMBParameters nmmbParams, Date currentDate) {
        LOGGER_UMO_MODEL.info("Enter UMO Model process");

        /* Prepare execution **************************************************************/
        nmmbParams.prepareUMOMOdelExecution(currentDate);
        MessagePrinter umoModelMP = new MessagePrinter(LOGGER_UMO_MODEL);

        /* Begin MPI call ***********************************************************/
        umoModelMP.printHeaderMsg("BEGIN");
        umoModelMP.printInfoMsg("Executing nmmb_esmf.x UMO-NMMb-DUST-RRTM model");

        Integer nemsEV = MPI.nems();

        LOGGER_UMO_MODEL.debug("Execution of mpirun NEMS ended with status " + nemsEV);
        if (nemsEV != 0) {
            LOGGER_UMO_MODEL.error("[ERROR] Error executing mpirun nems ");
            LOGGER_UMO_MODEL.error("Aborting...");
            System.exit(1);
        }
        umoModelMP.printInfoMsg("Finished Executing nmmb_esmf.x UMO-NMMb-DUST-RRTM model");

        /* Post execution **************************************************************/
        nmmbParams.postUMOModelExecution(currentDate);

        umoModelMP.printHeaderMsg("END");

        LOGGER_UMO_MODEL.info("UMO Model process finished");
    }

    /**
     * Performs the POST step
     * 
     */
    private static void doPost(NMMBParameters nmmbParams, Date currentDate) {
        String currentDateSTR = NMMBConstants.STR_TO_DATE.format(currentDate);
        LOGGER_POST.info("Postproc_carbono process for DAY: " + currentDateSTR);
        
        /* Prepare execution **************************************************************/
        nmmbParams.preparePostProcessExecution(currentDate);
        MessagePrinter postProcMP = new MessagePrinter(LOGGER_POST);
        
        /* Begin MPI call ***********************************************************/
        postProcMP.printHeaderMsg("BEGIN");

        // TODO ./run-postproc_auth.sh $FOLDER_OUTPUT glob/reg ${DATE}${HOUR}
        
        /* Post execution **************************************************************/
        nmmbParams.postPostProcessExecution(currentDate);

        postProcMP.printHeaderMsg("END");
        
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

            // Create output folders if needed
            nmmbParams.createOutputFolders(currentDate);

            // Vrbl process
            if (nmmbParams.DO_VRBL) {
                doVariable(nmmbParams, currentDate);
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
