package es.bsc.compss.worker.invokers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.worker.invokers.BinaryRunner.StreamSTD;


public class GenericInvoker {

    private static final int NUM_BASE_MPI_ARGS = 6;
    private static final int NUM_BASE_OMPSS_ARGS = 1;
    private static final int NUM_BASE_BINARY_ARGS = 1;
    private static final int NUM_BASE_DECAF_ARGS = 10;
    private static final String OMP_NUM_THREADS = "OMP_NUM_THREADS";


    /**
     * Invokes an MPI method
     * 
     * @param mpiRunner
     * @param mpiBinary
     * @param values
     * @param streams
     * @param prefixes
     * @param taskSandboxWorkingDir
     * @return
     * @throws InvokeExecutionException
     */
    public static Object invokeMPIMethod(String mpiRunner, String mpiBinary, Object[] values, Stream[] streams, String[] prefixes,
            File taskSandboxWorkingDir) throws InvokeExecutionException {

        System.out.println("");
        System.out.println("[MPI INVOKER] Begin MPI call to " + mpiBinary);
        System.out.println("[MPI INVOKER] On WorkingDir : " + taskSandboxWorkingDir.getAbsolutePath());

        // Command similar to
        // export OMP_NUM_THREADS=1 ; mpirun -H COMPSsWorker01,COMPSsWorker02 -n
        // 2 (--bind-to core) exec args

        // Get COMPSS ENV VARS
        String workers = System.getProperty(Constants.COMPSS_HOSTNAMES);
        String numNodes = System.getProperty(Constants.COMPSS_NUM_NODES);
        String computingUnits = System.getProperty(Constants.COMPSS_NUM_THREADS);
        String numProcs = String.valueOf(Integer.valueOf(numNodes) * Integer.valueOf(computingUnits));
        System.out.println("[MPI INVOKER] COMPSS HOSTNAMES: " + workers);
        System.out.println("[MPI INVOKER] COMPSS_NUM_NODES: " + numNodes);
        System.out.println("[MPI INVOKER] COMPSS_NUM_THREADS: " + computingUnits);

        // Convert binary parameters and calculate binary-streams redirection
        StreamSTD streamValues = new StreamSTD();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(values, streams, prefixes, streamValues);

        // Prepare command
        String[] cmd = new String[NUM_BASE_MPI_ARGS + binaryParams.size()];
        cmd[0] = mpiRunner;
        cmd[1] = "-H";
        cmd[2] = workers;
        cmd[3] = "-n";
        cmd[4] = numProcs;
        // cmd[5] = "--bind-to";
        // cmd[6] = "core";
        cmd[5] = mpiBinary;
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
        System.out.println("[MPI INVOKER] MPI STDIN: " + streamValues.getStdIn());
        System.out.println("[MPI INVOKER] MPI STDOUT: " + streamValues.getStdOut());
        System.out.println("[MPI INVOKER] MPI STDERR: " + streamValues.getStdErr());

        // Launch command
        return BinaryRunner.executeCMD(cmd, streamValues, taskSandboxWorkingDir);
    }

    /**
     * Invokes a Decaf method
     * 
     * @param dfRunner
     * @param dfScript
     * @param dfExecutor
     * @param dfLib
     * @param mpiRunner
     * @param values
     * @param streams
     * @param prefixes
     * @param taskSandboxWorkingDir
     * @return
     * @throws InvokeExecutionException
     */
    public static Object invokeDecafMethod(String dfRunner, String dfScript, String dfExecutor, String dfLib, String mpiRunner,
            Object[] values, Stream[] streams, String[] prefixes, File taskSandboxWorkingDir) throws InvokeExecutionException {

        System.out.println("");
        System.out.println("[DECAF INVOKER] Begin DECAF call to " + dfScript);
        System.out.println("[DECAF INVOKER] On WorkingDir : " + taskSandboxWorkingDir.getAbsolutePath());

        // Command similar to
        // export OMP_NUM_THREADS=1 ; mpirun -H COMPSsWorker01,COMPSsWorker02 -n
        // 2 (--bind-to core) exec args

        // Get COMPSS ENV VARS
        String workers = System.getProperty(Constants.COMPSS_HOSTNAMES);
        String numNodes = System.getProperty(Constants.COMPSS_NUM_NODES);
        String computingUnits = System.getProperty(Constants.COMPSS_NUM_THREADS);
        String numProcs = String.valueOf(Integer.valueOf(numNodes) * Integer.valueOf(computingUnits));
        System.out.println("[DECAF INVOKER] COMPSS HOSTNAMES: " + workers);
        System.out.println("[DECAF INVOKER] COMPSS_NUM_NODES: " + numNodes);
        System.out.println("[DECAF INVOKER] COMPSS_NUM_THREADS: " + computingUnits);

        // Convert binary parameters and calculate binary-streams redirection
        StreamSTD streamValues = new StreamSTD();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(values, streams, prefixes, streamValues);
        String hostfile = writeHostfile(taskSandboxWorkingDir, workers);
        // Prepare command
        String[] cmd = new String[NUM_BASE_DECAF_ARGS];
        cmd[0] = dfRunner;
        cmd[1] = dfScript;
        cmd[2] = dfExecutor;
        cmd[3] = dfLib;
        cmd[4] = mpiRunner;
        cmd[5] = "-n";
        cmd[6] = numProcs;
        cmd[7] = "--hostfile";
        cmd[8] = hostfile;

        String args = new String();
        for (int i = 0; i < binaryParams.size(); ++i) {
            if (i == 0) {
                args = args.concat(binaryParams.get(i));
            } else {
                args = args.concat(" " + binaryParams.get(i));
            }
        }
        cmd[9] = "--args=\"" + args + "\"";

        // Prepare environment
        System.setProperty(OMP_NUM_THREADS, computingUnits);

        // Debug command
        System.out.print("[DECAF INVOKER] Decaf CMD: ");
        for (int i = 0; i < cmd.length; ++i) {
            System.out.print(cmd[i] + " ");
        }
        System.out.println("");
        System.out.println("[DECAF INVOKER] Decaf STDIN: " + streamValues.getStdIn());
        System.out.println("[DECAF INVOKER] Decaf STDOUT: " + streamValues.getStdOut());
        System.out.println("[DECAF INVOKER] Decaf STDERR: " + streamValues.getStdErr());

        // Launch command
        return BinaryRunner.executeCMD(cmd, streamValues, taskSandboxWorkingDir);
    }

    private static String writeHostfile(File taskSandboxWorkingDir, String workers) throws InvokeExecutionException {
        String filename = taskSandboxWorkingDir.getAbsolutePath() + File.separator + ".decafHostfile";
        String workersInLines = workers.replace(',', '\n');
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(filename));
            writer.write(workersInLines);
        } catch (IOException e) {
            throw new InvokeExecutionException("Error writing decaf hostfile", e);
        } finally {
            try {
                if (writer != null)
                    writer.close();
            } catch (IOException e) {
                // Nothing to do
            }
        }
        return filename;
    }

    /**
     * Invokes an OmpSs method
     * 
     * @param ompssBinary
     * @param values
     * @param streams
     * @param prefixes
     * @param taskSandboxWorkingDir
     * @return
     * @throws InvokeExecutionException
     */
    public static Object invokeOmpSsMethod(String ompssBinary, Object[] values, Stream[] streams, String[] prefixes,
            File taskSandboxWorkingDir) throws InvokeExecutionException {

        System.out.println("");
        System.out.println("[OMPSS INVOKER] Begin ompss call to " + ompssBinary);
        System.out.println("[OMPSS INVOKER] On WorkingDir : " + taskSandboxWorkingDir.getAbsolutePath());

        // Get COMPSS ENV VARS
        String computingUnits = System.getProperty(Constants.COMPSS_NUM_THREADS);
        System.out.println("[OMPSS INVOKER] COMPSS_NUM_THREADS: " + computingUnits);

        // Command similar to
        // ./exec args

        // Convert binary parameters and calculate binary-streams redirection
        StreamSTD streamValues = new StreamSTD();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(values, streams, prefixes, streamValues);

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
        System.out.println("[OMPSS INVOKER] OmpSs STDIN: " + streamValues.getStdIn());
        System.out.println("[OMPSS INVOKER] OmpSs STDOUT: " + streamValues.getStdOut());
        System.out.println("[OMPSS INVOKER] OmpSs STDERR: " + streamValues.getStdErr());

        // Launch command
        return BinaryRunner.executeCMD(cmd, streamValues, taskSandboxWorkingDir);
    }

    /**
     * Invokes a binary method
     * 
     * @param binary
     * @param values
     * @param streams
     * @param prefixes
     * @param taskSandboxWorkingDir
     * @return
     * @throws InvokeExecutionException
     */
    public static Object invokeBinaryMethod(String binary, Object[] values, Stream[] streams, String[] prefixes, File taskSandboxWorkingDir)
            throws InvokeExecutionException {

        System.out.println("");
        System.out.println("[BINARY INVOKER] Begin binary call to " + binary);
        System.out.println("[BINARY INVOKER] On WorkingDir : " + taskSandboxWorkingDir.getAbsolutePath());

        // Command similar to
        // ./exec args

        // Convert binary parameters and calculate binary-streams redirection
        StreamSTD streamValues = new StreamSTD();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(values, streams, prefixes, streamValues);

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
        System.out.println("[BINARY INVOKER] Binary STDIN: " + streamValues.getStdIn());
        System.out.println("[BINARY INVOKER] Binary STDOUT: " + streamValues.getStdOut());
        System.out.println("[BINARY INVOKER] Binary STDERR: " + streamValues.getStdErr());

        // Launch command
        return BinaryRunner.executeCMD(cmd, streamValues, taskSandboxWorkingDir);
    }

}
