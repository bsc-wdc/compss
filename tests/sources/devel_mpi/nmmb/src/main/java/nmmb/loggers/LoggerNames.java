package nmmb.loggers;

/**
 * Contains the logger names
 * 
 */
public class LoggerNames {

    // Root logger
    public static final String NMMB = "NMMB";

    // Main class
    public static final String NMMB_MAIN = NMMB + ".Main";

    // Logger per phase
    public static final String NMMB_FIXED = NMMB + ".Fixed";
    public static final String NMMB_VARIABLE = NMMB + ".Variable";
    public static final String NMMB_UMO_MODEL = NMMB + ".UmoModel";
    public static final String NMMB_POST = NMMB + ".Post";

    // Logger for bach CMDs
    public static final String BASH_CMD = NMMB + ".BashCMDExecutor";

}
