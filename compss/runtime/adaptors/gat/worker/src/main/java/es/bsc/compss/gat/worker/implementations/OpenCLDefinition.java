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

import es.bsc.compss.gat.worker.GATWorker;
import es.bsc.compss.gat.worker.ImplementationDefinition;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.util.ErrorManager;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


public class OpenCLDefinition implements ImplementationDefinition {

    private final String kernel;

    public OpenCLDefinition(String kernel) {
        this.kernel = kernel;
    }

    @Override
    public MethodType getType() {
        return MethodType.OPENCL;
    }

    @Override
    public String toCommandString() {
        return kernel;
    }

    @Override
    public String toLogString() {
        return "["
                + "BINARY=" + kernel
                + "]";
    }

    @Override
    public Object process(Object target, Class<?>[] types, Object[] values, boolean[] areFiles, Stream[] streams, String[] prefixes, File sandBoxDir) {
        Object retValue = null;
        ErrorManager.error("ERROR: OpenCL is not supported");
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
