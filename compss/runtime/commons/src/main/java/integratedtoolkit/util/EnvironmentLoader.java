package integratedtoolkit.util;

import java.io.File;


/**
 * Support class to load environment variables
 *
 */
public class EnvironmentLoader {

    private static final String PREFIX_ENV_VAR = "$";
    private static final String END_ENV_VAR = "}";

    private static final String PREFIX_ENV_VAR_SCAPED = "\\$";
    private static final String BEGIN_ENV_VAR_SCAPED = "\\{";
    private static final String END_ENV_VAR_SCAPED = "\\}";


    /**
     * Loads the environment value of the given variable @variable if it is a variable (start with $) Otherwise it
     * returns the variable value
     * 
     * @param variable
     * @return
     */
    public static String loadFromEnvironment(String expression) {
        String expressionValue = expression;

        while (expressionValue != null && expressionValue.contains(PREFIX_ENV_VAR)) {
            // Compute the start of the env variable name
            int beginIndex = expressionValue.indexOf(PREFIX_ENV_VAR);
            // Check if the env variable name ends with END_ENV_VAR
            int endIndex = expressionValue.indexOf(END_ENV_VAR, beginIndex);
            if (endIndex == -1) {
                // Otherwise check if the variable name ends with the File separator
                endIndex = expressionValue.indexOf(File.separator, beginIndex);
                if (endIndex == -1) {
                    // Otherwise it ends at the end of the variable
                    endIndex = expressionValue.length();
                }
            } else {
                // Add one to remove also the final END_ENV_VAR
                endIndex = endIndex + 1;
            }

            // Retrieve the env var name
            String variableFullName = expressionValue.substring(beginIndex, endIndex);
            String variableLoadName = variableFullName.replaceAll(PREFIX_ENV_VAR_SCAPED, "");
            variableLoadName = variableLoadName.replaceAll(BEGIN_ENV_VAR_SCAPED, "");
            variableLoadName = variableLoadName.replaceAll(END_ENV_VAR_SCAPED, "");

            // Retrieve env var value
            String variableValue = System.getenv(variableLoadName);

            // Substitute on varValue
            if (variableValue != null) {
                expressionValue = expressionValue.replace(variableFullName, variableValue);
            } else {
                ErrorManager.warn("[WARNING] Null value obtained while loading " + variableLoadName + " from environment");
            }
        }

        return expressionValue;
    }

}
