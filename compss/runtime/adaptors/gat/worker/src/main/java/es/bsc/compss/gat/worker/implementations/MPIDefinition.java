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
package es.bsc.compss.gat.worker.implementations;

import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.gat.worker.GATWorker;
import es.bsc.compss.gat.worker.ImplementationDefinition;
import es.bsc.compss.invokers.util.BinaryRunner;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.util.ErrorManager;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;


public class MPIDefinition implements ImplementationDefinition {

    private static final int NUM_BASE_MPI_ARGS = 6;
    private static final String OMP_NUM_THREADS = "OMP_NUM_THREADS";

    private final String mpiRunner;
    private final String mpiBinary;

    public MPIDefinition(String mpiRunner, String mpiBinary) {
        this.mpiRunner = mpiRunner;
        this.mpiBinary = mpiBinary;
    }

    @Override
    public MethodType getType() {
        return MethodType.MPI;
    }

    @Override
    public String toCommandString() {
        return mpiRunner + " " + mpiBinary;
    }

    @Override
    public String toLogString() {
        return "["
                + "MPI RUNNER=" + mpiRunner
                + ", BINARY=" + mpiBinary
                + "]";
    }

    @Override
    public Object process(Object target, Class<?>[] types, Object[] values, boolean[] areFiles, Stream[] streams, String[] prefixes, File sandBoxdir) {
        Object retValue = null;
        try {

            System.out.println("");
            System.out.println("[MPI INVOKER] Begin MPI call to " + mpiBinary);
            System.out.println("[MPI INVOKER] On WorkingDir : " + sandBoxdir.getAbsolutePath());

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
            BinaryRunner.StreamSTD streamValues = new BinaryRunner.StreamSTD();
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
            retValue = BinaryRunner.executeCMD(cmd, streamValues, sandBoxdir, System.out, System.err);
        } catch (InvokeExecutionException iee) {
            ErrorManager.error(ERROR_INVOKE, iee);
        }
        boolean isFile = areFiles[areFiles.length - 1];
        String lastParamPrefix = prefixes[prefixes.length - 1];
        String lastParamName = (String) values[values.length - 1];
        serializeBinaryExitValue(retValue, isFile, lastParamPrefix, lastParamName);
        return retValue;
    }

    public static void serializeBinaryExitValue(Object retValue, boolean isFile, String lastParamPrefix, String lastParamName) {
        System.out.println("Checking binary exit value serialization");

        if (GATWorker.debug) {
            System.out.println("- Param isFile: " + isFile);
            System.out.println("- Prefix: " + lastParamPrefix);
        }

        // Last parameter is a FILE with skip prefix => return in Python
        // We cannot check it is OUT direction in GAT
        if (isFile && lastParamPrefix.equals(Constants.PREFIX_SKIP)) {
            // Write exit value to the file
            System.out.println("Writing Binary Exit Value (" + retValue.toString() + ") to " + lastParamName);

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(lastParamName))) {
                String value = "I" + retValue.toString() + "\n.\n";
                writer.write(value);
                writer.flush();
            } catch (IOException ioe) {
                System.err.println("ERROR: Cannot serialize binary exit value for bindings");
                ioe.printStackTrace();
            }
        }
    }
}
