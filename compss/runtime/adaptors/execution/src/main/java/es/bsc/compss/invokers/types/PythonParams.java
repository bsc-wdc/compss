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
package es.bsc.compss.invokers.types;

import es.bsc.compss.types.execution.LanguageParams;


/**
 * Python related variables
 *
 */
public class PythonParams implements LanguageParams {

    private String pythonInterpreter;
    private String pythonVersion;
    private String pythonVirtualEnvironment;
    private String pythonPropagateVirtualEnvironment;
    private String pythonPath;


    public PythonParams(String pythonInterpreter, String pythonVersion, String pythonVirtualEnvironment,
            String pythonPropagateVirtualEnvironment, String pythonPath) {

        this.pythonInterpreter = pythonInterpreter;
        this.pythonVersion = pythonVersion;
        this.pythonVirtualEnvironment = pythonVirtualEnvironment;
        this.pythonPropagateVirtualEnvironment = pythonPropagateVirtualEnvironment;
        this.pythonPath = pythonPath.equals("null") ? "" : pythonPath;
    }

    public String getPythonInterpreter() {
        return pythonInterpreter;
    }

    public void setPythonInterpreter(String pythonInterpreter) {
        this.pythonInterpreter = pythonInterpreter;
    }

    public String getPythonVersion() {
        return pythonVersion;
    }

    public void setPythonVersion(String pythonVersion) {
        this.pythonVersion = pythonVersion;
    }

    public String getPythonVirtualEnvironment() {
        return pythonVirtualEnvironment;
    }

    public void setPythonVirtualEnvironment(String pythonVirtualEnvironment) {
        this.pythonVirtualEnvironment = pythonVirtualEnvironment;
    }

    public String getPythonPropagateVirtualEnvironment() {
        return pythonPropagateVirtualEnvironment;
    }

    public void setPythonPropagateVirtualEnvironment(String pythonPropagateVirtualEnvironment) {
        this.pythonPropagateVirtualEnvironment = pythonPropagateVirtualEnvironment;
    }

    public String getPythonPath() {
        return pythonPath;
    }

    public void setPythonPath(String pythonPath) {
        this.pythonPath = pythonPath;
    }

}
