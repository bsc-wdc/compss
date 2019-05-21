/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.types.execution.exceptions.UnloadableValueException;
import es.bsc.compss.types.execution.exceptions.UnwritableValueException;
import es.bsc.distrostreamlib.server.types.StreamBackend;

import java.io.PrintStream;


public interface InvocationContext {

    // WORKER CONFIGURATION
    /**
     * Returns the node name.
     * 
     * @return The node name.
     */
    public String getHostName();

    /**
     * Returns the trancing node id.
     * 
     * @return The tracing ndoe id.
     */
    public long getTracingHostID();

    /**
     * Returns the COMPSs installation directory in the current node.
     * 
     * @return The COMPSs installation directory in the current node.
     */
    public String getInstallDir();

    /**
     * Returns the working directory in the current node.
     * 
     * @return The working directory in the current node.
     */
    public String getWorkingDir();

    /**
     * Returns the application directory.
     * 
     * @return The application directory.
     */
    public String getAppDir();

    // EXECUTION CONFIGURATION
    /**
     * Returns the task execution type.
     * 
     * @return The task execution type.
     */
    public TaskExecution getExecutionType();

    /**
     * Returns whether the persistent C worker is enabled or not.
     * 
     * @return {@literal true} if the persistent C worker is enabled, {@literal false} otherwise.
     */
    public boolean isPersistentCEnabled();

    /**
     * Returns the specific language parameters.
     * 
     * @param language Language.
     * @return The specific language parameters.
     */
    public LanguageParams getLanguageParams(Lang language);

    // EXECUTION MANAGEMENT
    /**
     * Registers the job outputs.
     * 
     * @param outputsBasename Base directory.
     */
    public void registerOutputs(String outputsBasename);

    /**
     * Unregisters the job outputs.
     */
    public void unregisterOutputs();

    /**
     * Returns the standard path to the streams.
     * 
     * @param invocation Task invocation.
     * @return The standard path to the streams.
     */
    public String getStandardStreamsPath(Invocation invocation);

    /**
     * Returns the Std OUT print stream.
     * 
     * @return The Std OUT print stream.
     */
    public PrintStream getThreadOutStream();

    /**
     * Returns the Std ERR print stream.
     * 
     * @return The Std ERR print stream.
     */
    public PrintStream getThreadErrStream();

    // DATA MANAGEMENT
    /**
     * Returns the path to the storage configuration.
     * 
     * @return The path to the storage configuration.
     */
    public String getStorageConf();

    /**
     * Returns the streaming backend.
     * 
     * @return The streaming backend.
     */
    public StreamBackend getStreamingBackend();

    /**
     * Returns the streaming master name.
     * 
     * @return The streaming master name.
     */
    public String getStreamingMasterName();

    /**
     * Returns the streaming master port.
     * 
     * @return The streaming master port.
     */
    public int getStreamingMasterPort();

    /**
     * Loads the given parameter.
     * 
     * @param np Parameter to load.
     * @throws UnloadableValueException When value cannot be loaded into memory.
     */
    public void loadParam(InvocationParam np) throws UnloadableValueException;

    /**
     * Stores the given parameter.
     * 
     * @param np Parameter to store.
     */
    public void storeParam(InvocationParam np) throws UnwritableValueException;

}
