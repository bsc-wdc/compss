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


public class OMPSsDefinition implements ImplementationDefinition {

    private static final int NUM_BASE_OMPSS_ARGS = 1;
    private static final String OMP_NUM_THREADS = "OMP_NUM_THREADS";

    private final String binary;

    public OMPSsDefinition(String binary) {
        this.binary = binary;
    }

    @Override
    public MethodType getType() {
        return MethodType.OMPSS;
    }

    @Override
    public String toCommandString() {
        return binary;
    }

    @Override
    public String toLogString() {
        return "["
                + "BINARY=" + binary
                + "]";
    }

    @Override
    public Object process(Object target, Class<?>[] types, Object[] values, boolean[] areFiles, Stream[] streams, String[] prefixes, File sandBoxDir) {
        Object retValue = null;
        try {

            System.out.println("");
            System.out.println("[OMPSS INVOKER] Begin ompss call to " + binary);
            System.out.println("[OMPSS INVOKER] On WorkingDir : " + sandBoxDir.getAbsolutePath());

            // Get COMPSS ENV VARS
            String computingUnits = System.getProperty(Constants.COMPSS_NUM_THREADS);
            System.out.println("[OMPSS INVOKER] COMPSS_NUM_THREADS: " + computingUnits);

            // Command similar to
            // ./exec args
            // Convert binary parameters and calculate binary-streams redirection
            BinaryRunner.StreamSTD streamValues = new BinaryRunner.StreamSTD();
            ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(values, streams, prefixes, streamValues);

            // Prepare command
            String[] cmd = new String[NUM_BASE_OMPSS_ARGS + binaryParams.size()];
            cmd[0] = binary;
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
