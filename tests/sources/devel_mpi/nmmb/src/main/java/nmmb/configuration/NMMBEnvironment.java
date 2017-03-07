package nmmb.configuration;

import java.io.File;


/**
 * Retrieve information from environment variables
 * 
 */
public class NMMBEnvironment {

    public static final String UMO_ROOT = System.getenv(NMMBConstants.ENV_NAME_UMO_ROOT) + File.separator;
    public static final String UMO_PATH = System.getenv(NMMBConstants.ENV_NAME_UMO_PATH) + File.separator;

    public static final String OUTNMMB = System.getenv(NMMBConstants.ENV_NAME_OUTNMMB) + File.separator;
    public static final String UMO_OUT = System.getenv(NMMBConstants.ENV_NAME_UMO_OUT) + File.separator;
    public static final String OUTPUT = System.getenv(NMMBConstants.ENV_NAME_OUTPUT) + File.separator;

    public static final String FIX = System.getenv(NMMBConstants.ENV_NAME_FIX) + File.separator;
    public static final String GEODATA_DIR = FIX + ".." + File.separator + "geodata" + File.separator;
    public static final String GTOPO_DIR = FIX + ".." + File.separator + "GTOPO30" + File.separator;
    public static final String FIX_INCLUDE_DIR = FIX + "include" + File.separator;

    public static final String VRB = System.getenv(NMMBConstants.ENV_NAME_VRB) + File.separator;
    public static final String VRB_INCLUDE_DIR = VRB + "include" + File.separator;

    public static final String UMO_LIBS = UMO_PATH + "MODEL" + File.separator + "libs" + File.separator;

    public static final String FNL = System.getenv(NMMBConstants.ENV_NAME_FNL) + File.separator;
    public static final String GFS = System.getenv(NMMBConstants.ENV_NAME_GFS) + File.separator;
    
    public static final String SRCDIR = System.getenv(NMMBConstants.ENV_NAME_SRCDIR) + File.separator;
    public static final String CHEMIC = System.getenv(NMMBConstants.ENV_NAME_CHEMIC) + File.separator;
    public static final String DATMOD = System.getenv(NMMBConstants.ENV_NAME_DATMOD) + File.separator;
    
    public static final String POST_CARBONO = System.getenv(NMMBConstants.ENV_NAME_POST_CARBONO) + File.separator;

    // ITF constants
    public static final String FIX_FOR_ITF = "/home/bsc19/bsc19533/nmmb/nmmb-bsc-ctm-v2.0/PREPROC/FIXED/";
    public static final String LOOKUP_TABLES_DIR_FOR_ITF = "/home/bsc19/bsc19533/nmmb/nmmb-bsc-ctm-v2.0/PREPROC/FIXED/lookup_tables/";
    public static final String VRB_FOR_ITF = "/home/bsc19/bsc19533/nmmb/nmmb-bsc-ctm-v2.0/PREPROC/VARIABLE/";
    public static final String EXE_FOR_ITF = "/gpfs/projects/bsc19/bsc19533/NMMB-BSC/nmmb-bsc-ctm-v2.0/MODEL/exe/";

}
