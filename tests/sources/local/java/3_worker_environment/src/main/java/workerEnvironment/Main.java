package workerEnvironment;

public class Main {

    private static final String CLASSPATH = "CLASSPATH";
    // private static final String PYTHONPATH = "PYTHONPATH";
    private static final String LD_LIBRARY_PATH = "LD_LIBRARY_PATH";


    private static boolean checkEnvironment(String variable, String expected) {
        String variableValue = System.getenv(variable);

        System.out.println("Checking " + variable);
        System.out.println(" - Got: " + variableValue);
        System.out.println(" - Exp: " + expected);

        return variableValue.contains(expected);
    }

    public static boolean checkAllEnv(String extraCPExpected, String extraPPExpected, String extraLPExpected) {
        if (!checkEnvironment(CLASSPATH, extraCPExpected)) {
            System.err.println("ERROR: Classpath variable not set correctly");
            return false;
        }

        // Not set on java applications
        // if (!checkEnvironment(PYTHONPATH, extraPPExpected)) {
        // System.err.println("ERROR: Pythonpath variable not set correctly");
        // return false;
        // }

        if (!checkEnvironment(LD_LIBRARY_PATH, extraLPExpected)) {
            System.err.println("ERROR: Library Path variable not set correctly");
            return false;
        }

        // All ok
        return true;
    }

    public static void main(String[] args) {
        // Retrieve the arguments of the application
        String extraCPmasterExpected = args[0];
        String extraPPmasterExpected = args[1];
        String extraLPmasterExpected = args[2];

        // Check the environment on the master side
        if (!checkAllEnv(extraCPmasterExpected, extraPPmasterExpected, extraLPmasterExpected)) {
            System.err.println("ERROR: Invalid Master environment");
            return;
        }

        // Launch task to check the worker side
        if (!MainImpl.checkWorkerEnv()) {
            System.err.println("ERROR: Invalid Worker environment");
            return;
        }

        // All ok
        System.out.println("[LOG] ALL ENVIRONMENT OK");
    }
}
