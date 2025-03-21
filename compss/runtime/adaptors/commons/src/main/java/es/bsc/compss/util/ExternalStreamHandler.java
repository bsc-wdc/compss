/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.util;

import es.bsc.compss.COMPSsDefaults;
import es.bsc.compss.exceptions.ExternalPropertyException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;


public class ExternalStreamHandler {

    /**
     * Launches a ProcessBuilder with a Python command to deserialize and retrieve the value of the given property.
     *
     * @param pythonInterpreter Python Interpreter.
     * @param fileName File containing the serialized object.
     * @param property Property name.
     * @return Property value.
     * @throws ExternalPropertyException When the property value cannot be retrieved due to missing file, serialization
     *             issues or invalid property.
     */
    public static String getExternalStreamProperty(String pythonInterpreter, String fileName, String property)
        throws ExternalPropertyException {

        // Checking if running in coverage mode
        if (pythonInterpreter.startsWith("coverage ") || pythonInterpreter.startsWith("coverage#")) {
            pythonInterpreter = COMPSsDefaults.PYTHON_INTERPRETER;
        } else if (pythonInterpreter.startsWith("coverage3")) {
            pythonInterpreter = "python3";
        }

        // Build Python call
        StringBuilder pythonCall = new StringBuilder();
        pythonCall.append("import pickle;");
        pythonCall.append("pickle_in=open('").append(fileName).append("', 'rb');");
        pythonCall.append("pickle_in.seek(4);");
        pythonCall.append("obj = pickle.load(pickle_in);");
        pythonCall.append("print(obj.").append(property).append(");");
        pythonCall.append("pickle_in.close()");
        pythonCall.append("");

        // Build command
        String[] cmd = new String[3];
        cmd[0] = pythonInterpreter;
        cmd[1] = "-c";
        cmd[2] = pythonCall.toString();

        // Build ProcessBuilder
        ProcessBuilder builder = new ProcessBuilder(cmd);

        // Setup process environment -- Tracing entries
        Tracer.prepareEnvironment(builder.environment(), false);

        // Execute command
        Process process;
        String propertyValue = null;
        try {
            // Create process
            process = builder.start();

            // Disable inputs to process
            process.getOutputStream().close();

            // Wait and retrieve exit value
            int exitValue = process.waitFor();
            if (exitValue != 0) {
                String errorMsg = "Process exit value = " + exitValue + "\n";
                String internalError = getStreamContent(process.getErrorStream());
                errorMsg = errorMsg + internalError;
                throw new ExternalPropertyException(errorMsg);
            } else {
                propertyValue = getStreamContent(process.getInputStream());
            }
        } catch (IOException ioe) {
            throw new ExternalPropertyException(ioe);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ExternalPropertyException(ie);
        }

        return propertyValue;
    }

    /**
     * Processes the content of an input stream.
     *
     * @param is Input stream to process.
     * @return String representing the content of the input stream.
     * @throws IOException When an IO error occurs reading the input stream.
     */
    private static String getStreamContent(InputStream is) throws IOException {
        StringBuilder content = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                content.append(line);
            }
        }

        return content.toString();
    }

}
