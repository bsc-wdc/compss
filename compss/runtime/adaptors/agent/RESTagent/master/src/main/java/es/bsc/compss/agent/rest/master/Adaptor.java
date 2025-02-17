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
package es.bsc.compss.agent.rest.master;

import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.NodeMonitor;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.types.resources.jaxb.ResourcesExternalAdaptorProperties;
import es.bsc.compss.types.resources.jaxb.ResourcesPropertyAdaptorType;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.conn.types.StarterCommand;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.core.script.Script;


public class Adaptor implements CommAdaptor {

    @Override
    public void init() {

    }

    @Override
    public Configuration constructConfiguration(Map<String, Object> projectConf, Map<String, Object> resourcesConf)
        throws ConstructConfigurationException {

        AgentConfiguration ac = new AgentConfiguration(this.getClass().getName());

        ResourcesExternalAdaptorProperties eap = (ResourcesExternalAdaptorProperties) resourcesConf.get("Properties");
        for (ResourcesPropertyAdaptorType prop : eap.getProperty()) {
            ac.addProperty(prop.getName(), prop.getValue());
        }
        /*
         * ac.setHost((String) props.get("host")); MethodResourceDescription mrd = (MethodResourceDescription)
         * props.get("description"); ac.setDescription(mrd); ac.setLimitOfTasks(mrd.getTotalCPUComputingUnits());
         */
        return ac;
    }

    @Override
    public COMPSsWorker initWorker(Configuration config, NodeMonitor monitor) {
        AgentConfiguration ac = (AgentConfiguration) config;
        return new RemoteRESTAgent(ac, monitor);
    }

    @Override
    public void stop() {
        // You cannot stop a remote agent.
    }

    @Override
    public List<DataOperation> getPending() {
        // There are never pending data operations since you never copy data onto it
        return new LinkedList<>();
    }

    @Override
    public void completeMasterURI(MultiURI muri) {
        // NO need to do anything since everything is copied through the StorageItf
    }

    @Override
    public void stopSubmittedJobs() {
        // You can't do that
    }

    @Override
    public StarterCommand getStarterCommand(String workerName, int workerPort, String masterName, String workingDir,
        String installDir, String appDir, String classpathFromFile, String pythonpathFromFile, String libPathFromFile,
        String envScriptFromFile, String pythonInterpreterFromFile, int totalCPU, int totalGPU, int totalFPGA,
        int limitOfTasks, String hostId) {
        return null;
    }

}
