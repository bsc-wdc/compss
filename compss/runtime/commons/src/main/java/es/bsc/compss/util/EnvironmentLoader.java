/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.util;

import java.io.File;


/**
 * Support class to load environment variables.
 */
public class EnvironmentLoader {

    private static final String PREFIX_ENV_VAR = "$";
    private static final String BEGIN_ENV_VAR = "}";
    private static final String END_ENV_VAR = "}";

    private static final String PREFIX_ENV_VAR_SCAPED = "\\$";
    private static final String BEGIN_ENV_VAR_SCAPED = "\\{";
    private static final String END_ENV_VAR_SCAPED = "\\}";


    /**
     * Evaluates the given expression {@code expression}. If it is a variable (starts with $), loads its value from the
     * environment. Otherwise, returns the original expression value.
     * 
     * @param expression Variable expression.
     * @return Value after resolving the variable expression.
     */
    public static String loadFromEnvironment(String expression) {
        String expressionValue = expression;

        int beginIndex = findNextValidToken(expressionValue, 0);
        while (beginIndex >= 0) {
            // Check whether it is scaped or must be treated
            boolean isScaped = (beginIndex >= 1 && expressionValue.charAt(beginIndex - 1) == '\\');

            if (isScaped) {
                // Remove the scape
                StringBuilder sb = new StringBuilder(expressionValue);
                sb.deleteCharAt(beginIndex - 1);
                expressionValue = sb.toString();
            } else {
                // Retrieve end index
                int endIndex = findEndIndex(expressionValue, beginIndex);

                // Retrieve end var full name
                String variableFullName = expressionValue.substring(beginIndex, endIndex);

                // Retrieve env var value
                String variableValue = loadEnvVariable(variableFullName);

                // Substitute on varValue
                if (variableValue != null) {
                    expressionValue = expressionValue.replace(variableFullName, variableValue);
                } else {
                    ErrorManager
                        .warn("[WARNING] Null value obtained while loading " + variableFullName + " from environment");
                    expressionValue = expressionValue.replace(variableFullName, "");
                }
            }

            // Process next token
            beginIndex = findNextValidToken(expressionValue, beginIndex);
        }

        return expressionValue;
    }

    /**
     * Finds the position of the next valid token inside expressionValue that is bigger than beginIndex.
     * 
     * @param expressionValue Expression value.
     * @param beginIndex Begin index.
     * @return Index of the next valid token.
     */
    private static int findNextValidToken(String expressionValue, int beginIndex) {
        // Find next appearance
        return expressionValue.indexOf(PREFIX_ENV_VAR, beginIndex);
    }

    /**
     * Finds the end index from the token's start position.
     * 
     * @param expressionValue Expression value.
     * @param beginIndex Begin index.
     * @return Index of the end of the token.
     */
    private static int findEndIndex(String expressionValue, int beginIndex) {
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

        return endIndex;
    }

    /**
     * Parses the variable name and loads its value from the environment.
     * 
     * @param variableFullName Variable name.
     * @return Environment value of the given variable.
     */
    private static String loadEnvVariable(String variableFullName) {
        // Retrieve the env var name
        String variableLoadName = variableFullName.replaceAll(PREFIX_ENV_VAR_SCAPED, "");
        variableLoadName = variableLoadName.replaceAll(BEGIN_ENV_VAR_SCAPED, "");
        variableLoadName = variableLoadName.replaceAll(END_ENV_VAR_SCAPED, "");

        // Retrieve env var value
        String variableValue = System.getenv(variableLoadName);
        return variableValue;
    }

}
