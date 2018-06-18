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


public class DecafDefinition implements ImplementationDefinition {

    private static final int NUM_BASE_DECAF_ARGS = 11;
    private static final String OMP_NUM_THREADS = "OMP_NUM_THREADS";

    private final String dfRunner;
    private final String dfScript;
    private final String dfExecutor;
    private final String dfLib;
    private final String mpiRunner;

    public DecafDefinition(String dfPath, String dfScript, String dfExecutor, String dfLib, String mpiRunner) {
        this.dfRunner = dfPath;
        this.dfScript = dfScript;
        this.dfExecutor = dfExecutor;
        this.dfLib = dfLib;
        this.mpiRunner = mpiRunner;
    }

    @Override
    public MethodType getType() {
        return MethodType.DECAF;
    }

    @Override
    public String toCommandString() {
        return dfRunner + " " + dfScript + " " + dfExecutor + " " + dfLib + " " + mpiRunner;
    }

    @Override
    public String toLogString() {
        return "["
                + "DF RUNNER=" + dfRunner
                + "DF SCRIPT=" + dfScript
                + "DF EXECUTOR=" + dfExecutor
                + "DF LIB=" + dfLib
                + "MPI RUNNER=" + mpiRunner
                + "]";
    }

    @Override
    public Object process(Object target, Class<?>[] types, Object[] values, boolean[] areFiles, Stream[] streams, String[] prefixes, File sandBoxDir) {
        Object retValue = null;
        try {

            System.out.println("");
            System.out.println("[DECAF INVOKER] Begin DECAF call to " + dfScript);
            System.out.println("[DECAF INVOKER] On WorkingDir : " + sandBoxDir.getAbsolutePath());

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
            BinaryRunner.StreamSTD streamValues = new BinaryRunner.StreamSTD();
            ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(values, streams, prefixes, streamValues);
            String hostfile = writeHostfile(sandBoxDir, workers);
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
            retValue = BinaryRunner.executeCMD(cmd, streamValues, sandBoxDir, System.out, System.err);
        } catch (InvokeExecutionException iee) {
            ErrorManager.error(ERROR_INVOKE, iee);
        }
        boolean isFile = areFiles[areFiles.length - 1];
        String lastParamPrefix = prefixes[prefixes.length - 1];
        String lastParamName = (String) values[values.length - 1];
        serializeBinaryExitValue(retValue, isFile, lastParamPrefix, lastParamName);
        return retValue;
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
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                // Nothing to do
            }
        }
        return filename;
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
