package testMPI;

import integratedtoolkit.types.annotations.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import testMPI.types.MPIExecutionResult;


public class MainImpl {

    private static final String ERROR_CLOSE_READER = "ERROR: Cannot close reader CMD output";
    private static final String ERROR_PROC_EXEC = "ERROR: Exception executing MPI command";
    private static final String ERROR_EXIT_VAL = "ERROR: Cannot execute MPI command. ExitValue = ";

    private static final int NUM_BASE_MPI_ARGS = 8;
    private static final String OMP_NUM_THREADS = "OMP_NUM_THREADS";


    public static int taskSingleMPI(String binary, int[] data) {
        return taskMPI(binary, data);
    }

    public static int taskMultipleMPI(String binary, int[] data) {
        return taskMPI(binary, data);
    }

    private static int taskMPI(String binary, int[] data) {
        MPIExecutionResult result = executeMPICommand(binary, data);

        int exitValue = result.getExitValue();
        System.out.println("MPI CMD EXIT VALUE: " + exitValue);
        if (exitValue != 0) {
            System.err.println(ERROR_EXIT_VAL + exitValue);
            System.err.println("PROCESS OUTPUT: ");
            System.err.println(result.getOutputMessage());
            System.err.println("PROCESS ERROR: ");
            System.err.println(result.getErrorMessage());

            return -1;
        }

        // The execution has finished properly
        String value = result.getValueFromOutput();
        System.out.println("RECEIVED VALUE: " + value);
        return (int) Integer.valueOf(value);
    }

    private static MPIExecutionResult executeMPICommand(String binary, int[] data) {
        // Command similar to
        // export OMP_NUM_THREADS=1 ; mpirun -H COMPSsWorker01,COMPSsWorker02 -n 2 --bind-to core exec args

        // Get COMPSS ENV VARS
        String workers = System.getProperty(Constants.COMPSS_HOSTNAMES);
        String numNodes = System.getProperty(Constants.COMPSS_NUM_NODES);
        String computingUnits = System.getProperty(Constants.COMPSS_NUM_THREADS);
        System.out.println("COMPSS HOSTNAMES: " + workers);
        System.out.println("COMPSS_NUM_NODES: " + numNodes);
        System.out.println("COMPSS_NUM_THREADS: " + computingUnits);

        // Prepare command
        String[] cmd = new String[NUM_BASE_MPI_ARGS + data.length];
        cmd[0] = "mpirun";
        cmd[1] = "-H";
        cmd[2] = workers;
        cmd[3] = "-n";
        cmd[4] = numNodes;
        cmd[5] = "--bind-to";
        cmd[6] = "core";
        cmd[7] = binary;
        for (int i = 0; i < data.length; ++i) {
            cmd[NUM_BASE_MPI_ARGS + i] = String.valueOf(data[i]);
        }

        // Prepare environment
        System.setProperty(OMP_NUM_THREADS, computingUnits);

        // Debug command
        System.out.print("MPI CMD: ");
        for (int i = 0; i < cmd.length; ++i) {
            System.out.print(cmd[i] + " ");
        }
        System.out.println("");

        // Launch command
        MPIExecutionResult result = new MPIExecutionResult();
        Process p;
        BufferedReader reader = null;
        try {
            // Execute command
            p = Runtime.getRuntime().exec(cmd);

            // Wait for completion and retrieve exitValue
            int exitValue = p.waitFor();
            result.setExitValue(exitValue);

            // Store any process output
            StringBuffer output = new StringBuffer();
            reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = "";
            while ((line = reader.readLine()) != null) {
                output.append(line + "\n");
            }
            result.setOutputMessage(output.toString());

            // Store any process error
            StringBuffer error = new StringBuffer();
            reader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            line = "";
            while ((line = reader.readLine()) != null) {
                error.append(line + "\n");
            }
            result.setErrorMessage(error.toString());
        } catch (Exception e) {
            result.setExitValue(-1);
            result.setErrorMessage(ERROR_PROC_EXEC);

            System.err.println(ERROR_PROC_EXEC);
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    System.err.println(ERROR_CLOSE_READER);
                    e.printStackTrace();
                }
            }
        }

        // Return
        return result;
    }

}
