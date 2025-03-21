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
package es.bsc.compss.comm;

import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.NodeMonitor;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.conn.types.StarterCommand;

import java.util.List;
import java.util.Map;


/**
 * Abstract definition of a Communication Adaptor for the Runtime.
 */
public interface CommAdaptor {

    /**
     * Initializes the Communication Adaptor.
     */
    public void init();

    /**
     * Creates a configuration instance for the specific adaptor.
     * 
     * @param projectProperties Properties from the project.xml file.
     * @param resourcesProperties Properties from the resources.xml file.
     * @return Adaptor configuration.
     * @throws ConstructConfigurationException When cannot load the adaptor jar files.
     */
    public Configuration constructConfiguration(Map<String, Object> projectProperties,
        Map<String, Object> resourcesProperties) throws ConstructConfigurationException;

    /**
     * Creates a worker starter command with the given information.
     * 
     * @param workerName Worker name.
     * @param workerPort Worker port.
     * @param masterName Master name.
     * @param workingDir Worker working directory.
     * @param installDir Installation directory.
     * @param appDir Application directory.
     * @param classpathFromFile Classpath.
     * @param pythonpathFromFile Pythonpath.
     * @param libPathFromFile Librarypath.
     * @param envScriptFromFile Environment script path.
     * @param pythonInterpreterFromFile Python interpreter.
     * @param totalCPU Total number of CPUs.
     * @param totalGPU Total number of GPUs.
     * @param totalFPGA Total number of FPGAs.
     * @param limitOfTasks Limit of tasks.
     * @param hostId Tracing host id.
     * @return The worker start command.
     */
    public StarterCommand getStarterCommand(String workerName, int workerPort, String masterName, String workingDir,
        String installDir, String appDir, String classpathFromFile, String pythonpathFromFile, String libPathFromFile,
        String envScriptFromFile, String pythonInterpreterFromFile, int totalCPU, int totalGPU, int totalFPGA,
        int limitOfTasks, String hostId);

    /**
     * Initializes a worker through an adaptor.
     * 
     * @param config Adaptor configuration.
     * @param monitor element monitoring changes on the node
     * @return A COMPSsWorker object representing the initialized worker.
     */
    public COMPSsWorker initWorker(Configuration config, NodeMonitor monitor);

    /**
     * Retrieves all the pending operations.
     * 
     * @return All the pending operations.
     */
    public List<DataOperation> getPending();

    /**
     * Modifies the given MultiURI to append the complete Master URI.
     * 
     * @param u MultiURI to store the Master URI.
     */
    public void completeMasterURI(MultiURI u);

    /**
     * Stops all the pending jobs inside the Communication Adaptor.
     */
    public void stopSubmittedJobs();

    /**
     * Stops the Communication Adaptor.
     */
    public void stop();

}
