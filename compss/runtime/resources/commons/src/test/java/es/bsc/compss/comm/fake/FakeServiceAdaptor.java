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
package es.bsc.compss.comm.fake;

import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.NodeMonitor;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.fake.FakeNode;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.types.resources.configuration.ServiceConfiguration;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.conn.types.StarterCommand;

import java.util.LinkedList;
import java.util.Map;


public class FakeServiceAdaptor implements CommAdaptor {

    @Override
    public void init() {

    }

    @Override
    public Configuration constructConfiguration(Map<String, Object> projectProperties,
        Map<String, Object> resourcesProperties) throws ConstructConfigurationException {

        return new ServiceConfiguration(this.getClass().getName(), "WSDL");
    }

    @Override
    public COMPSsWorker initWorker(Configuration config, NodeMonitor monitor) {
        ServiceConfiguration serviceCfg = (ServiceConfiguration) config;
        return new FakeNode(serviceCfg.getWsdl(), monitor);

    }

    @Override
    public void stop() {
        // Nothing to do
    }

    @Override
    public LinkedList<DataOperation> getPending() {
        return new LinkedList<>();
    }

    @Override
    public void completeMasterURI(MultiURI u) {
        // Nothing to do
    }

    @Override
    public void stopSubmittedJobs() {
        // Nothing to do
    }

    @Override
    public StarterCommand getStarterCommand(String workerName, int workerPort, String masterName, String workingDir,
        String installDir, String appDir, String classpathFromFile, String pythonpathFromFile, String libPathFromFile,
        String envScriptPathFromFile, String pythonInterpreterFromFile, int totalCPU, int totalGPU, int totalFPGA,
        int limitOfTasks, String hostId) {
        return null;
    }

}
