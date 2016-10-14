package integratedtoolkit.worker.invokers;

import java.util.ArrayList;

import integratedtoolkit.types.annotations.Constants;


public class GenericInvoker {

    private static final int NUM_BASE_MPI_ARGS = 8;
    private static final int NUM_BASE_OMPSS_ARGS = 1;
    private static final int NUM_BASE_BINARY_ARGS = 1;

    private static final String OMP_NUM_THREADS = "OMP_NUM_THREADS";


    public static Object invokeMPIMethod(String mpiRunner, String mpiBinary, Object[] values) throws InvokeExecutionException {
        System.out.println("[MPI INVOKER] Begin MPI call to " + mpiBinary);

        // Command similar to
        // export OMP_NUM_THREADS=1 ; mpirun -H COMPSsWorker01,COMPSsWorker02 -n 2 --bind-to core exec args

        // Get COMPSS ENV VARS
        String workers = System.getProperty(Constants.COMPSS_HOSTNAMES);
        String numNodes = System.getProperty(Constants.COMPSS_NUM_NODES);
        String computingUnits = System.getProperty(Constants.COMPSS_NUM_THREADS);
        System.out.println("[MPI INVOKER] COMPSS HOSTNAMES: " + workers);
        System.out.println("[MPI INVOKER] COMPSS_NUM_NODES: " + numNodes);
        System.out.println("[MPI INVOKER] COMPSS_NUM_THREADS: " + computingUnits);

        // Convert binary parameters
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(values);

        // Prepare command
        String[] cmd = new String[NUM_BASE_MPI_ARGS + binaryParams.size()];
        cmd[0] = mpiRunner;
        cmd[1] = "-H";
        cmd[2] = workers;
        cmd[3] = "-n";
        cmd[4] = numNodes;
        cmd[5] = "--bind-to";
        cmd[6] = "core";
        cmd[7] = mpiBinary;
        for (int i = 0; i < binaryParams.size(); ++i) {
            cmd[NUM_BASE_MPI_ARGS + i] = binaryParams.get(i);
        }

        // Prepare environment
        System.setProperty(OMP_NUM_THREADS, computingUnits);

        // Debug command
        System.out.print("[MPI INVOKER] MPI CMD: ");
        for (int i = 0; i < cmd.length; ++i) {
            System.out.print(cmd[i] + " ");
        }
        System.out.println("");

        // Launch command
        return BinaryRunner.executeCMD(cmd);
    }

    public static Object invokeOmpSsMethod(String ompssBinary, Object[] values) throws InvokeExecutionException {
        System.out.println("[OMPSS INVOKER] Begin ompss call to " + ompssBinary);

        // Get COMPSS ENV VARS
        String computingUnits = System.getProperty(Constants.COMPSS_NUM_THREADS);
        System.out.println("[OMPSS INVOKER] COMPSS_NUM_THREADS: " + computingUnits);

        // Command similar to
        // ./exec args

        // Convert binary parameters
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(values);

        // Prepare command
        String[] cmd = new String[NUM_BASE_OMPSS_ARGS + binaryParams.size()];
        cmd[0] = ompssBinary;
        for (int i = 0; i < binaryParams.size(); ++i) {
            cmd[NUM_BASE_OMPSS_ARGS + i] = binaryParams.get(i);
        }

        // Prepare environment
        System.setProperty(OMP_NUM_THREADS, computingUnits);

        // Debug command
        System.out.print("[OMPSS INVOKER] BINARY CMD: ");
        for (int i = 0; i < cmd.length; ++i) {
            System.out.print(cmd[i] + " ");
        }
        System.out.println("");

        // Launch command
        return BinaryRunner.executeCMD(cmd);
    }

    public static Object invokeBinaryMethod(String binary, Object[] values) throws InvokeExecutionException {
        System.out.println("[BINARY INVOKER] Begin binary call to " + binary);

        // Command similar to
        // ./exec args

        // Convert binary parameters
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(values);

        // Prepare command
        String[] cmd = new String[NUM_BASE_BINARY_ARGS + binaryParams.size()];
        cmd[0] = binary;
        for (int i = 0; i < binaryParams.size(); ++i) {
            cmd[NUM_BASE_BINARY_ARGS + i] = binaryParams.get(i);
        }

        // Debug command
        System.out.print("[BINARY INVOKER] BINARY CMD: ");
        for (int i = 0; i < cmd.length; ++i) {
            System.out.print(cmd[i] + " ");
        }
        System.out.println("");

        // Launch command
        return BinaryRunner.executeCMD(cmd);
    }

}
