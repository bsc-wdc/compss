package nmmb.utils;

/**
 * Contains the filenames of the fortran executables used by the FIXED phase
 * 
 */
public class FortranWrapper {

    public static final String FC = "ifort";
    public static final String MC_FLAG = "-mcmodel=large";
    public static final String SHARED_FLAG = "-shared-intel";
    public static final String CONVERT_PREFIX = "-convert";
    public static final String CONVERT_VALUE = "big_endian";
    public static final String TRACEBACK_FLAG = "-traceback";
    public static final String ASSUME_PREFIX = "-assume";
    public static final String ASSUME_VALUE = "byterecl";
    public static final String OPT_FLAG = "-O3";
    public static final String FPMODEL_PREFIX = "-fp-model";
    public static final String FPMODEL_VALUE = "precise";
    public static final String STACK_FLAG = "-fp-stack-check";
    public static final String CFLAG = "-c";
    public static final String OFLAG = "-o";

    public static final String W3_LIB_DIR = "w3lib-2.0.2/";
    public static final String BACIO_LIB_DIR = "bacio/";
    public static final String W3_FLAG = "-lw3_4";
    public static final String BACIO_FLAG = "-lbacio_4";

    public static final String SUFFIX_F90_SRC = ".f90";
    public static final String SUFFIX_F_SRC = ".f";
    public static final String SUFFIX_OBJECT = ".o";
    public static final String SUFFIX_EXE = ".x";

    /*
     * FIXED FORTRAN FILES
     */
    public static final String BOTSOILTYPE = "botsoiltype";
    public static final String GFDLCO2 = "gfdlco2";
    public static final String DEEPTEMPERATURE = "deeptemperature";
    public static final String ENVELOPE = "envelope";
    public static final String LANDUSE = "landuse";
    public static final String LANDUSENEW = "landusenew";
    public static final String SMMOUNT = "smmount";
    public static final String ROUGHNESS = "roughness";
    public static final String STDH = "stdh";
    public static final String STDHTOPO = "stdhtopo";
    public static final String SNOWALBEDO = "snowalbedo";
    public static final String TOPO = "topo";
    public static final String TOPOSEAMASK = "toposeamask";
    public static final String TOPSOILTYPE = "topsoiltype";
    public static final String VCGENERATOR = "vcgenerator";

    public static final String[] FIXED_FORTRAN_F90_FILES = new String[] { BOTSOILTYPE, DEEPTEMPERATURE, ENVELOPE, LANDUSE, LANDUSENEW,
            SMMOUNT, ROUGHNESS, STDH, STDHTOPO, SNOWALBEDO, TOPO, TOPOSEAMASK, TOPSOILTYPE, VCGENERATOR };

    public static final String[] FIXED_FORTRAN_F_FILES = new String[] { GFDLCO2 };

    /*
     * VARIABLE FORTRAN FILES
     */
    public static final String ALBEDO = "albedo";
    public static final String ALBEDO_RRTM_1DEG = "albedorrtm1deg";
    public static final String ALLPREP_RRTM = "allprep_rrtm";
    public static final String CNV_RRTM = "cnv_rrtm";
    public static final String DEGRIB_SST = "degribsst";
    public static final String DUST_START = "dust_start";
    public static final String GFS2MODEL = "gfs2model_rrtm";
    public static final String INC_RRTM = "inc_rrtm";
    public static final String MODULE_FLT = "module_flt"; // used by all prep
    public static final String VEG_FRAC = "vegfrac";
    public static final String Z0_VEGUSTAR = "z0vegustar";

    public static final String[] VARIABLE_FORTRAN_F90_DEP_FILES = new String[] { MODULE_FLT };

    public static final String[] VARIABLE_FORTRAN_F90_FILES = new String[] { ALBEDO, ALBEDO_RRTM_1DEG, VEG_FRAC, Z0_VEGUSTAR };

    public static final String[] VARIABLE_FORTRAN_F_FILES = new String[] { CNV_RRTM, DUST_START, GFS2MODEL, INC_RRTM };

    public static final String[] VARIABLE_FORTRAN_F_FILES_WITH_DEPS = new String[] { ALLPREP_RRTM };

    public static final String[] VARIABLE_FORTRAN_F_FILES_WITH_W3 = new String[] { DEGRIB_SST };

}
