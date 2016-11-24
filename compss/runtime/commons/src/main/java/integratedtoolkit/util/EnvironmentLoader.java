package integratedtoolkit.util;

/**
 * Support class to load environment variables
 *
 */
public class EnvironmentLoader {

    private static final String PREFIX_ENV_VAR = "$";
    private static final String PREFIX_ENV_VAR_SCAPED = "\\$";


    /**
     * Loads the environment value of the given variable @variable if it is a variable (start with $) Otherwise it
     * returns the variable value
     * 
     * @param variable
     * @return
     */
    public static String loadFromEnvironment(String variable) {
        String varValue = variable;
        if (variable != null && variable.startsWith(PREFIX_ENV_VAR)) {
            varValue = variable.replaceAll(PREFIX_ENV_VAR_SCAPED, "");
            varValue = varValue.replaceAll("\\{", "");
            varValue = varValue.replaceAll("\\}", "");
            varValue = System.getenv(varValue);
        }

        return varValue;
    }

}
