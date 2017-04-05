package nmmb.configuration;

import java.io.File;


/**
 * Retrieve information from environment variables
 * 
 */
public class NMMBEnvironment {

    public static final String UMO_PATH = System.getenv(NMMBConstants.ENV_NAME_UMO_PATH) + File.separator;
    public static final String UMO_ROOT = System.getenv(NMMBConstants.ENV_NAME_UMO_ROOT) + File.separator;

    public static final String FIX = System.getenv(NMMBConstants.ENV_NAME_FIX) + File.separator;
    public static final String VRB = System.getenv(NMMBConstants.ENV_NAME_VRB) + File.separator;
    public static final String POST_CARBONO = System.getenv(NMMBConstants.ENV_NAME_POST_CARBONO) + File.separator;

    public static final String OUTPUT = System.getenv(NMMBConstants.ENV_NAME_OUTPUT) + File.separator;
    public static final String OUTNMMB = System.getenv(NMMBConstants.ENV_NAME_OUTNMMB) + File.separator;
    public static final String UMO_OUT = System.getenv(NMMBConstants.ENV_NAME_UMO_OUT) + File.separator;

    public static final String SRCDIR = System.getenv(NMMBConstants.ENV_NAME_SRCDIR) + File.separator;
    public static final String CHEMIC = System.getenv(NMMBConstants.ENV_NAME_CHEMIC) + File.separator;
    public static final String DATMOD = System.getenv(NMMBConstants.ENV_NAME_DATMOD) + File.separator;

    public static final String FNL = System.getenv(NMMBConstants.ENV_NAME_FNL) + File.separator;
    public static final String GFS = System.getenv(NMMBConstants.ENV_NAME_GFS) + File.separator;

    // Infer some FIXED paths
    public static final String GEODATA_DIR = FIX + ".." + File.separator + "geodata" + File.separator;
    public static final String GTOPO_DIR = FIX + ".." + File.separator + "GTOPO30" + File.separator;
    public static final String FIX_INCLUDE_DIR = FIX + "include" + File.separator;

    // Infer some VARIABLE paths
    public static final String VRB_INCLUDE_DIR = VRB + "include" + File.separator;

    // Infer some UMO Model paths
    public static final String UMO_LIBS = SRCDIR + "libs" + File.separator;

}
