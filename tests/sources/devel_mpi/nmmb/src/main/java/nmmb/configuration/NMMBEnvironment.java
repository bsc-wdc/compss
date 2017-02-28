package nmmb.configuration;

import java.io.File;


/**
 * Retrieve information from environment variables
 * 
 */
public class NMMBEnvironment {

    public static final String UMO_PATH = System.getenv(NMMBConstants.ENV_NAME_UMO_PATH) + File.separator;

    public static final String OUTNMMB = System.getenv(NMMBConstants.ENV_NAME_OUTNMMB) + File.separator;
    public static final String UMO_OUT = System.getenv(NMMBConstants.ENV_NAME_UMO_OUT) + File.separator;
    public static final String OUTPUT = System.getenv(NMMBConstants.ENV_NAME_OUTPUT);

    public static final String FIX = System.getenv(NMMBConstants.ENV_NAME_FIX) + File.separator;
    public static final String GEODATA_DIR = FIX + ".." + File.separator + "geodata" + File.separator;
    public static final String GTOPO_DIR = FIX + ".." + File.separator + "GTOPO30" + File.separator;
    public static final String INCLUDE_DIR = FIX + "include" + File.separator;

    // ITF constants
    public static final String FIX_FOR_ITF = "/home/bsc19/bsc19533/nmmb/nmmb-bsc-ctm-v2.0/PREPROC/FIXED/";
    public static final String LOOKUP_TABLES_DIR_FOR_ITF = "/home/bsc19/bsc19533/nmmb/nmmb-bsc-ctm-v2.0/PREPROC/FIXED/lookup_tables/";

}
