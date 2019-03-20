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
package es.bsc.compss.comm;

import java.util.List;

import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.types.uri.MultiURI;


/**
 * Abstract definition of a Communication Adaptor for the Runtime
 */
public interface CommAdaptor {

    /**
     * Initializes the Communication Adaptor
     */
    public void init();

    /**
     * Creates a configuration instance for the specific adaptor
     * 
     * @param project_properties
     * @param resources_properties
     * @return
     * @throws Exception
     */
    public Configuration constructConfiguration(Object project_properties, Object resources_properties)
            throws ConstructConfigurationException;

    /**
     * Initializes a worker through an adaptor
     * 
     * @param workerName
     * @param config
     * @return
     */
    public COMPSsWorker initWorker(String workerName, Configuration config);

    /**
     * Stops the Communication Adaptor
     */
    public void stop();

    /**
     * Retrieves all the pending operations
     * 
     * @return
     */
    public List<DataOperation> getPending();

    /**
     * Returns the complete Master URI
     * 
     * @param u
     */
    public void completeMasterURI(MultiURI u);

    /**
     * Stops all the pending jobs inside the Communication Adaptor
     */
    public void stopSubmittedJobs();

}
