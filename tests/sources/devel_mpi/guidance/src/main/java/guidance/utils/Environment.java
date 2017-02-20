package guidance.utils;

import java.io.File;

import guidance.exceptions.EnvironmentVariableException;


/**
 * Capture the Environment Variable values that contain the information of the applications that will be used in the
 * Guidance execution
 *
 */
public class Environment {

    public static final String EV_RSCRIPTBINDIR = "RSCRIPTBINDIR";
    public static final String EV_RSCRIPTDIR = "RSCRIPTDIR";

    public static final String EV_PLINKBINARY = "PLINKBINARY";
    public static final String EV_GTOOLBINARY = "GTOOLBINARY";
    public static final String EV_QCTOOLBINARY = "QCTOOLBINARY";
    public static final String EV_SHAPEITBINARY = "SHAPEITBINARY";
    public static final String EV_IMPUTE2BINARY = "IMPUTE2BINARY";
    public static final String EV_SNPTESTBINARY = "SNPTESTBINARY";

    public static final String EV_JAVA_HOME = "JAVA_HOME";


    /**
     * Environment variables and its values
     *
     */
    public static enum ENV_VARS {
        R_SCRIPT_BIN_DIR(System.getenv(EV_RSCRIPTBINDIR)), // R script bin dir
        R_SCRIPT_DIR(System.getenv(EV_RSCRIPTDIR)), // R script dir
        PLINK_BINARY(System.getenv(EV_PLINKBINARY)), // Plink
        GTOOL_BINARY(System.getenv(EV_GTOOLBINARY)), // GTool
        QCTOOL_BINARY(System.getenv(EV_QCTOOLBINARY)), // QCTool
        SHAPEIT_BINARY(System.getenv(EV_SHAPEITBINARY)), // Shapeit
        IMPUTE2_BINARY(System.getenv(EV_IMPUTE2BINARY)), // Impute2
        SNPTEST_BINARY(System.getenv(EV_SNPTESTBINARY)), // SNP Test
        JAVA_HOME(System.getenv(EV_JAVA_HOME)), // Java home
        JAVA_BINARY(JAVA_HOME.value() + File.separator + "java");

        private String envValue;


        private ENV_VARS(String envValue) {
            this.envValue = envValue;
        }

        /**
         * Returns the environment value of the variable
         * 
         * @return
         */
        public String value() {
            return this.envValue;
        }
    }


    /**
     * Verifies the environment
     * 
     * @throws EnvironmentVariableException
     */
    public static void verify() throws EnvironmentVariableException {
        // Check that variables are defined
        for (ENV_VARS envVar : ENV_VARS.values()) {
            String envVarValue = envVar.value();
            if (envVarValue == null) {
                throw new EnvironmentVariableException(
                        "[Guidance] Error, " + envVar + " environment variable in not present in .bashrc. You must define it properly");
            }
        }

        // Check binaries existence
        checkBinaryExistance(ENV_VARS.PLINK_BINARY.name(), ENV_VARS.PLINK_BINARY.value());
        checkBinaryExistance(ENV_VARS.GTOOL_BINARY.name(), ENV_VARS.GTOOL_BINARY.value());
        checkBinaryExistance(ENV_VARS.QCTOOL_BINARY.name(), ENV_VARS.QCTOOL_BINARY.value());
        checkBinaryExistance(ENV_VARS.SHAPEIT_BINARY.name(), ENV_VARS.SHAPEIT_BINARY.value());
        checkBinaryExistance(ENV_VARS.IMPUTE2_BINARY.name(), ENV_VARS.IMPUTE2_BINARY.value());
        checkBinaryExistance(ENV_VARS.SNPTEST_BINARY.name(), ENV_VARS.SNPTEST_BINARY.value());
    }

    private static void checkBinaryExistance(String envVarName, String envVarValue) throws EnvironmentVariableException {
        File f = new File(envVarValue);
        if (!f.exists() || f.isDirectory()) {
            throw new EnvironmentVariableException(
                    "[Guidance] Error, " + envVarName + "  environment variable in not present in .bashrc. You must define it properly");
        }
    }

}
