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
package es.bsc.compss.types.execution;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.COMPSsConstants.TaskExecution;
import java.io.PrintStream;


public interface InvocationContext {

    //WORKER CONFIGURATION
    public String getHostName();

    public long getTracingHostID();

    public String getAppDir();

    public String getInstallDir();

    public String getLibPath();

    public String getWorkingDir();

    //EXECUTION CONFIGURATION
    public TaskExecution getExecutionType();

    public boolean isPersistentEnabled();

    public LanguageParams getLanguageParams(Lang language);

    //EXECUTION MANAGEMENT
    public void registerOutputs(String outputsBasename);

    public void unregisterOutputs();

    public String getStandardStreamsPath(Invocation invocation);

    public PrintStream getThreadOutStream();

    public PrintStream getThreadErrStream();

    //DATA MANAGEMENT
    public String getStorageConf();

    public void loadParam(InvocationParam np) throws Exception;

    public void storeParam(InvocationParam np) throws Exception;

}
