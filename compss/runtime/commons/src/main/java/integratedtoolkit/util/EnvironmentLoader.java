package integratedtoolkit.util;

import java.io.File;


/**
 * Support class to load environment variables
 *
 */
public class EnvironmentLoader {

    private static final String PREFIX_ENV_VAR = "$";
    private static final String BEGIN_ENV_VAR = "}";
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
            
            // Compute the end of the ENV variable name
            int endIndex;
            // Check if the ENV variable uses {}
            if (beginIndex + 1 < expressionValue.length()) {
                if (expressionValue.substring(beginIndex, beginIndex + 1).equals(BEGIN_ENV_VAR)) {
                    // Env variable uses {}
                    endIndex = expressionValue.indexOf(END_ENV_VAR, beginIndex) + 1;
                } else {
                    // Env variable does not use {}
                    endIndex = expressionValue.indexOf(File.separator, beginIndex);
                    if (endIndex == -1) {
                        // Env variable is not in a path, ends with expressionValue end
                        endIndex = expressionValue.length();
                    }
                }
            } else {
                // Found an expression of the form "---$"
                endIndex = expressionValue.length();
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
                expressionValue = expressionValue.replace(variableFullName, "");
            }
        }

        return expressionValue;
    }

}
