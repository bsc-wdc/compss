package integratedtoolkit.api;

/**
 * COMPSs API Class for JAVA
 *
 */
public class COMPSs {

    private static final String SKIP_MESSAGE = "COMPSs Runtime is not loaded. Skipping call";


    /**
     * Barrier
     * 
     */
    public static void barrier() {
        // This is only a handler, it is never executed
        System.out.println(SKIP_MESSAGE);
    }

}
