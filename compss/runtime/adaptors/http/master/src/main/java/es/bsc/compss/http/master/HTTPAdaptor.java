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
package es.bsc.compss.http.master;

import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.NodeMonitor;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.types.resources.configuration.HTTPConfiguration;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.conn.types.StarterCommand;
import java.util.LinkedList;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


// Loaded dynamically
public class HTTPAdaptor implements CommAdaptor {

    public static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);


    @Override
    public void init() {
        try {
            HTTPJob.init();
        } catch (Exception e) {
            LOGGER.error("Can not initialize HTTP Adaptor");
        }
    }

    @Override
    public Configuration constructConfiguration(Map<String, Object> projectProperties,
        Map<String, Object> resourcesProperties) throws ConstructConfigurationException {

        es.bsc.compss.types.project.jaxb.HttpType hProject =
            (es.bsc.compss.types.project.jaxb.HttpType) projectProperties.get("Http");
        es.bsc.compss.types.resources.jaxb.HttpType hResources =
            (es.bsc.compss.types.resources.jaxb.HttpType) resourcesProperties.get("Http");

        String baseUrl = null;
        if (hProject != null) {
            baseUrl = hProject.getBaseUrl();
        } else if (hResources != null) {
            baseUrl = hResources.getBaseUrl();
        } else {
            // No base url (http service unique key), throw exception
            throw new ConstructConfigurationException("Cannot configure http service because no URL provided");
        }
        HTTPConfiguration config = new HTTPConfiguration(this.getClass().getName(), baseUrl);
        if (hProject != null) {
            config.setLimitOfTasks(hProject.getLimitOfTasks());
        }

        if (hResources != null) {
            config.setServices(hResources.getServiceName());
        }

        return config;
    }

    @Override
    public StarterCommand getStarterCommand(String workerName, int workerPort, String masterName, String workingDir,
        String installDir, String appDir, String classpathFromFile, String pythonpathFromFile, String libPathFromFile,
        String envScriptFromFile, String pythonInterpreterFromFile, int totalCPU, int totalGPU, int totalFPGA,
        int limitOfTasks, String hostId) {
        return null;
    }

    @Override
    public COMPSsWorker initWorker(Configuration config, NodeMonitor monitor) {
        HTTPConfiguration httpConfiguration = (HTTPConfiguration) config;

        String httpName = "httpẀorker";
        LOGGER.debug("Init HTTP Worker named " + httpName);

        return new HTTPInstance(httpConfiguration, monitor, httpName);
    }

    @Override
    public void stop() {
        HTTPJob.end();
    }

    @Override
    public LinkedList<DataOperation> getPending() {
        return null;
    }

    @Override
    public void stopSubmittedJobs() {

    }

    @Override
    public void completeMasterURI(MultiURI u) {
        // No need to do nothing
    }

}
