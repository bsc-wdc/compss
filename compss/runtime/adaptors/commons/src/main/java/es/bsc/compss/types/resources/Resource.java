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
package es.bsc.compss.types.resources;

import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.types.annotations.parameter.DataType;
import java.util.List;
import java.util.Set;


public interface Resource extends Comparable<Resource> {

    public enum Type {
        MASTER, // For the master node
        WORKER, // For the worker nodes
        SERVICE // For services
    }

    /**
     * Starts a resource execution
     *
     * @throws es.bsc.compss.exceptions.InitNodeException
     */
    public void start() throws InitNodeException;


    /**
     * Returns all the LogicalData stored in the host
     *
     * @param
     * @return
     */
    public Set<LogicalData> getAllDataFromHost();

    /**
     * Adds a new LogicalData available in the host
     *
     * @param ld
     */
    public void addLogicalData(LogicalData ld);

    /**
     * Marks a file as obsolete
     *
     * @param obsolete
     */
    public void addObsolete(LogicalData obsolete);

    /**
     * Gets the list of obsolete files
     *
     * @return List of logicalData objects
     */
    public LogicalData[] pollObsoletes();

    /**
     * Clears the list of obsolete files
     */
    public void clearObsoletes();

    /**
     * Returns the node name
     *
     * @return
     */
    public String getName();

    /**
     * Returns the node associated to the resource
     *
     * @return
     */
    public COMPSsNode getNode();

    /**
     * Returns the internal URI representation of the given MultiURI
     *
     * @param u
     * @throws UnstartedNodeException
     */
    public void setInternalURI(MultiURI u) throws UnstartedNodeException;

    /**
     * Creates a new Job
     *
     * @param taskId
     * @param taskParams
     * @param impl
     * @param slaveWorkersNodeNames
     * @param listener
     * @return
     */
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation impl, List<String> slaveWorkersNodeNames,
            JobListener listener);

    /**
     * Retrieves a given data
     *
     * @param dataId
     * @param tgtDataId
     * @param reason
     * @param listener
     */
    public void getData(String dataId, String tgtDataId, Transferable reason, EventListener listener);

    /**
     * Retrieves a given data
     *
     * @param ld
     * @param tgtData
     * @param reason
     * @param listener
     */
    public void getData(LogicalData ld, LogicalData tgtData, Transferable reason, EventListener listener);

    /**
     * Retrieves a given data
     *
     * @param dataId
     * @param newName
     * @param tgtDataId
     * @param reason
     * @param listener
     */
    public void getData(String dataId, String newName, String tgtDataId, Transferable reason, EventListener listener);

    /**
     * Retrieves a given data
     *
     * @param dataId
     * @param newName
     * @param tgtData
     * @param reason
     * @param listener
     */
    public void getData(String dataId, String newName, LogicalData tgtData, Transferable reason, EventListener listener);

    /**
     * Retrieves a given data
     *
     * @param ld
     * @param newName
     * @param tgtData
     * @param reason
     * @param listener
     */
    public void getData(LogicalData ld, String newName, LogicalData tgtData, Transferable reason, EventListener listener);

    /**
     * Retrieves a given data
     *
     * @param dataId
     * @param target
     * @param reason
     * @param listener
     */
    public void getData(String dataId, DataLocation target, Transferable reason, EventListener listener);

    public void getData(String dataId, DataLocation target, String tgtDataId, Transferable reason, EventListener listener);

    /**
     * Retrieves a given data
     *
     * @param dataId
     * @param target
     * @param tgtData
     * @param reason
     * @param listener
     */
    public void getData(String dataId, DataLocation target, LogicalData tgtData, Transferable reason, EventListener listener);

    /**
     * Retrieves a given data
     *
     * @param srcData
     * @param target
     * @param tgtData
     * @param reason
     * @param listener
     */
    public void getData(LogicalData srcData, DataLocation target, LogicalData tgtData, Transferable reason, EventListener listener);

    /**
     * Returns the complete remote path of a given data
     *
     * @param type
     * @param name
     * @return
     */
    public SimpleURI getCompleteRemotePath(DataType type, String name);

    /**
     * Retrieves all the data from the Resource
     *
     * @param saveUniqueData
     */
    public void retrieveData(boolean saveUniqueData);

    /**
     * Deletes the intermediate data
     *
     */
    public void deleteIntermediate();

    /**
     * Stops the resource
     *
     * @param sl
     */
    public void stop(ShutdownListener sl);

    /**
     * Returns the Resource type
     *
     * @return
     */
    public Type getType();

}
