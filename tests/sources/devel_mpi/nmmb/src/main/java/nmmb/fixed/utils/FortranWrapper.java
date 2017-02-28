package nmmb.fixed.utils;

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
    public static final String OFLAG = "-o";

    public static final String SUFFIX_F90_SRC = ".f90";
    public static final String SUFFIX_F_SRC = ".f";
    public static final String SUFFIX_EXE = ".x";

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

    public static final String[] FORTRAN_F90_FILES = new String[] { BOTSOILTYPE, DEEPTEMPERATURE, ENVELOPE, LANDUSE, LANDUSENEW, SMMOUNT,
            ROUGHNESS, STDH, STDHTOPO, SNOWALBEDO, TOPO, TOPOSEAMASK, TOPSOILTYPE, VCGENERATOR };

    public static final String[] FORTRAN_F_FILES = new String[] { GFDLCO2 };

}
