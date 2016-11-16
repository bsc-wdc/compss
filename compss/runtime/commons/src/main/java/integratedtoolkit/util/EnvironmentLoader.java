package integratedtoolkit.util;

public class EnvironmentLoader {

    private static final String PREFIX_ENV_VAR = "$";
    private static final String PREFIX_ENV_VAR_SCAPED = "\\$";


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
