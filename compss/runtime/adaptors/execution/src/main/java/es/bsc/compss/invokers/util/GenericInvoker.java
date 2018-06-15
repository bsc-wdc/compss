/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.invokers.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.UUID;

import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.invokers.util.BinaryRunner.StreamSTD;


public class GenericInvoker {

    private static final int NUM_BASE_MPI_ARGS = 6;
    private static final int NUM_BASE_DECAF_ARGS = 11;
    private static final int NUM_BASE_OMPSS_ARGS = 1;
    private static final int NUM_BASE_BINARY_ARGS = 1;

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
            File taskSandboxWorkingDir, PrintStream defaultOutStream, PrintStream defaultErrStream) throws InvokeExecutionException {

        System.out.println("");
        System.out.println("[MPI INVOKER] Begin MPI call to " + mpiBinary);
        System.out.println("[MPI INVOKER] On WorkingDir : " + taskSandboxWorkingDir.getAbsolutePath());

        // Command similar to
        // export OMP_NUM_THREADS=1 ; mpirun -hostfile hostfile_path -n 2 (--bind-to core) exec args

        // Get COMPSS ENV VARS
        String workers = System.getProperty(Constants.COMPSS_HOSTNAMES);
        String numNodes = System.getProperty(Constants.COMPSS_NUM_NODES);
        String computingUnits = System.getProperty(Constants.COMPSS_NUM_THREADS);
        String numProcs = String.valueOf(Integer.valueOf(numNodes) * Integer.valueOf(computingUnits));
        System.out.println("[MPI INVOKER] COMPSS HOSTNAMES: " + workers);
        System.out.println("[MPI INVOKER] COMPSS_NUM_NODES: " + numNodes);
        System.out.println("[MPI INVOKER] COMPSS_NUM_THREADS: " + computingUnits);

        // Create hostfile
        String hostfile = writeHostfile(taskSandboxWorkingDir, workers);

        // Convert binary parameters and calculate binary-streams redirection
        StreamSTD streamValues = new StreamSTD();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(values, streams, prefixes, streamValues);

        // Prepare command
        String[] cmd = new String[NUM_BASE_MPI_ARGS + binaryParams.size()];
        cmd[0] = mpiRunner;
        cmd[1] = "-hostfile";
        cmd[2] = hostfile;
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
        return BinaryRunner.executeCMD(cmd, streamValues, taskSandboxWorkingDir, defaultOutStream, defaultErrStream);
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
            Object[] values, Stream[] streams, String[] prefixes, File taskSandboxWorkingDir, PrintStream defaultOutStream,
            PrintStream defaultErrStream) throws InvokeExecutionException {

        System.out.println("");
        System.out.println("[DECAF INVOKER] Begin DECAF call to " + dfScript);
        System.out.println("[DECAF INVOKER] On WorkingDir : " + taskSandboxWorkingDir.getAbsolutePath());

        // Get COMPSS ENV VARS
        String workers = System.getProperty(Constants.COMPSS_HOSTNAMES);
        String numNodes = System.getProperty(Constants.COMPSS_NUM_NODES);
        String computingUnits = System.getProperty(Constants.COMPSS_NUM_THREADS);
        String numProcs = String.valueOf(Integer.valueOf(numNodes) * Integer.valueOf(computingUnits));
        System.out.println("[DECAF INVOKER] COMPSS HOSTNAMES: " + workers);
        System.out.println("[DECAF INVOKER] COMPSS_NUM_NODES: " + numNodes);
        System.out.println("[DECAF INVOKER] COMPSS_NUM_THREADS: " + computingUnits);

        // Create hostfile
        String hostfile = writeHostfile(taskSandboxWorkingDir, workers);

        // Convert binary parameters and calculate binary-streams redirection
        StreamSTD streamValues = new StreamSTD();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(values, streams, prefixes, streamValues);

        // Prepare command
        String args = new String();
        for (int i = 0; i < binaryParams.size(); ++i) {
            if (i == 0) {
                args = args.concat(binaryParams.get(i));
            } else {
                args = args.concat(" " + binaryParams.get(i));
            }
        }
        String[] cmd;
        if (args.isEmpty()) {
            cmd = new String[NUM_BASE_DECAF_ARGS - 2];
        } else {
            cmd = new String[NUM_BASE_DECAF_ARGS];
        }
        cmd[0] = dfRunner;
        cmd[1] = dfScript;
        cmd[2] = dfExecutor;
        cmd[3] = dfLib;
        cmd[4] = mpiRunner;
        cmd[5] = "-n";
        cmd[6] = numProcs;
        cmd[7] = "--hostfile";
        cmd[8] = hostfile;
        if (!args.isEmpty()) {
            cmd[9] = "--args=\"";
            cmd[10] = args;
        }

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
        return BinaryRunner.executeCMD(cmd, streamValues, taskSandboxWorkingDir, defaultOutStream, defaultErrStream);
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
            File taskSandboxWorkingDir, PrintStream defaultOutStream, PrintStream defaultErrStream) throws InvokeExecutionException {

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
        return BinaryRunner.executeCMD(cmd, streamValues, taskSandboxWorkingDir, defaultOutStream, defaultErrStream);
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
    public static Object invokeBinaryMethod(String binary, Object[] values, Stream[] streams, String[] prefixes, File taskSandboxWorkingDir,
            PrintStream defaultOutStream, PrintStream defaultErrStream) throws InvokeExecutionException {

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
        return BinaryRunner.executeCMD(cmd, streamValues, taskSandboxWorkingDir, defaultOutStream, defaultErrStream);
    }

    /**
     * Writes the given list of workers to a hostfile inside the given task sandbox
     * 
     * @param taskSandboxWorkingDir
     * @param workers
     * @return
     * @throws InvokeExecutionException
     */
    private static String writeHostfile(File taskSandboxWorkingDir, String workers) throws InvokeExecutionException {
        // Locate hostfile file
        String uuid = UUID.randomUUID().toString();
        String filename = taskSandboxWorkingDir.getAbsolutePath() + File.separator + uuid + ".hostfile";

        // Modify the workers' list
        String workersInLines = workers.replace(',', '\n');

        // Write hostfile
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            writer.write(workersInLines);
        } catch (IOException ioe) {
            throw new InvokeExecutionException("ERROR: Cannot write hostfile", ioe);
        }
        return filename;
    }

}
