package workerEnvironment;

public class MainImpl {

    public static boolean checkWorkerEnv() {
        // This values are according to the project.xml file
        final String extraCPworkerExpected = "/tmp/extraClasspath/worker";
        final String extraPPworkerExpected = "/tmp/extraPythonpath/worker";
        final String extraLPworkerExpected = "/tmp/extraLibrarypath/worker";

        return Main.checkAllEnv(extraCPworkerExpected, extraPPworkerExpected, extraLPworkerExpected);
    }

}
