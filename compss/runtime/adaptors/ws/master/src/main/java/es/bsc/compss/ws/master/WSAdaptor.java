/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.ws.master;

import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.NodeMonitor;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.types.resources.jaxb.PriceType;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.ws.master.configuration.WSConfiguration;
import es.bsc.conn.types.StarterCommand;

import java.util.LinkedList;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class WSAdaptor implements CommAdaptor {

    // Logging
    public static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);


    @Override
    public void init() {
        try {
            WSJob.init();
        } catch (Exception e) {
            LOGGER.error("Can not initialize WS Adaptor");
        }
    }

    @Override
    public Configuration constructConfiguration(Map<String, Object> projectProperties,
        Map<String, Object> resourcesProperties) throws ConstructConfigurationException {

        es.bsc.compss.types.project.jaxb.ServiceType sProject =
            (es.bsc.compss.types.project.jaxb.ServiceType) projectProperties.get("Service");
        es.bsc.compss.types.resources.jaxb.ServiceType sResources =
            (es.bsc.compss.types.resources.jaxb.ServiceType) resourcesProperties.get("Service");

        String wsdl = null;
        if (sProject != null) {
            wsdl = sProject.getWsdl();
        } else if (sResources != null) {
            wsdl = sResources.getWsdl();
        } else {
            // No wsdl (service unique key), throw exception
            throw new ConstructConfigurationException("Cannot configure service because no WSDL provided");
        }

        WSConfiguration config = new WSConfiguration(this.getClass().getName(), wsdl);
        if (sProject != null) {
            config.setLimitOfTasks(sProject.getLimitOfTasks());
        }

        if (sResources != null) {
            config.setServiceName(sResources.getName());
            config.setNamespace(sResources.getNamespace());
            String servicePort = sResources.getPort();
            if (servicePort != null && !servicePort.isEmpty()) {
                config.setPort(sResources.getPort());
            }
            PriceType p = sResources.getPrice();
            if (p != null) {
                config.setPricePerUnitTime(p.getPricePerUnit());
                config.setPriceUnitTime(p.getTimeUnit());
            }
        }

        return config;
    }

    @Override
    public COMPSsWorker initWorker(Configuration config, NodeMonitor monitor) {
        WSConfiguration wsCfg = (WSConfiguration) config;
        LOGGER.debug("Init WS Worker Node named " + wsCfg.getWsdl());
        return new ServiceInstance(wsCfg, monitor);
    }

    @Override
    public void stop() {
        WSJob.end();
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

    @Override
    public StarterCommand getStarterCommand(String workerName, int workerPort, String masterName, String workingDir,
        String installDir, String appDir, String classpathFromFile, String pythonpathFromFile, String libPathFromFile,
        String envScriptFromFile, String pythonInterpreterFromFile, int totalCPU, int totalGPU, int totalFPGA,
        int limitOfTasks, String hostId) {
        return null;
    }

}
