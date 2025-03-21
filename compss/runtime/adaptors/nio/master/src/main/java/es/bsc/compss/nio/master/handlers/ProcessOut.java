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
package es.bsc.compss.nio.master.handlers;

public class ProcessOut {

    private StringBuffer output;
    private StringBuffer error;
    private int exitValue;


    /**
     * Creates a new ProcessOut instance.
     */
    public ProcessOut() {
        this.output = new StringBuffer();
        this.error = new StringBuffer();
        this.exitValue = -1;
    }

    /**
     * Returns the process exit value.
     * 
     * @return The process exit value.
     */
    public int getExitValue() {
        return this.exitValue;
    }

    /**
     * Returns the process output.
     * 
     * @return The process output.
     */
    public String getOutput() {
        return this.output.toString();
    }

    /**
     * Returns the process error.
     * 
     * @return The process error.
     */
    public String getError() {
        return this.error.toString();
    }

    /**
     * Sets a new process exit value.
     * 
     * @param exit The new process exit value.
     */
    public void setExitValue(int exit) {
        this.exitValue = exit;
    }

    /**
     * Adds a new line to the process error.
     * 
     * @param line New error line.
     */
    public void appendError(String line) {
        this.error.append(line);
    }

    /**
     * Adds a new line to the process output.
     * 
     * @param line New output line.
     */
    public void appendOutput(String line) {
        this.output.append(line + "\n");
    }

}
