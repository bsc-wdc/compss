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

package es.bsc.compss.data;

import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.exceptions.InitializationException;
import es.bsc.compss.types.execution.exceptions.UnloadableValueException;
import es.bsc.distrostreamlib.server.types.StreamBackend;

import java.util.List;


public interface DataManager {

    /**
     * Initializes the DataManager.
     * 
     * @throws InitializationException When any internal component fails to start.
     */
    public void init() throws InitializationException;

    /**
     * Stops the DataManager.
     * 
     * @throws InterruptedException if the current thread is interrupted whe trying to use a semaphore
     */
    public void stop();

    /**
     * Returns the storage configuration file path.
     * 
     * @return The storage configuration file path. {@code null} if none was provided.
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
     * Removes the given obsolete objects.
     * 
     * @param obsoletes List of obsolete objects.
     */
    public void removeObsoletes(List<String> obsoletes);

    /**
     * Retrieves the given parameter.
     * 
     * @param param Invocation Parameter information.
     * @param i Parameter position.
     * @param tt Listener for completion.
     */
    public void fetchParam(InvocationParam param, int i, FetchDataListener tt);

    /**
     * Loads the given parameter into memory.
     * 
     * @param param Invocation Parameter information.
     * @throws UnloadableValueException When value cannot be loaded to memory.
     */
    public void loadParam(InvocationParam param) throws UnloadableValueException;

    /**
     * Stores the given parameter information.
     * 
     * @param param Invocation Parameter information.
     */
    public void storeParam(InvocationParam param);

    /**
     * Returns the object associated to the given data management id {@code dataMgmtId}.
     * 
     * @param dataMgmtId Data Management id.
     * @return Associated object.
     */
    public Object getObject(String dataMgmtId);

    /**
     * Stores the given value {@code value} for the given parameter {@code name}.
     * 
     * @param name Parameter name.
     * @param value Associated value.
     */
    public void storeValue(String name, Object value);

    /**
     * Stores the given {@code file} for the given parameter {@code dataId}.
     * 
     * @param dataId Parameter id.
     * @param string File path.
     */
    public void storeFile(String dataId, String string);

}
