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
package es.bsc.compss.agent.comm;

import es.bsc.comm.Connection;
import es.bsc.comm.nio.NIONode;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.agent.comm.messages.types.CommResource;
import es.bsc.compss.agent.comm.messages.types.CommTask;
import es.bsc.compss.agent.types.RemoteDataLocation;
import es.bsc.compss.agent.types.Resource;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.NIOTaskResult;
import es.bsc.compss.nio.master.NIOAdaptor;
import es.bsc.compss.nio.master.NIOJob;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.NodeMonitor;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.job.JobHistory;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;
import es.bsc.compss.types.resources.jaxb.ResourcesExternalAdaptorProperties;
import es.bsc.compss.types.resources.jaxb.ResourcesPropertyAdaptorType;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Class handling all the Adaptor requests to manage Comm Agent Workers.
 */
public class CommAgentAdaptor extends NIOAdaptor implements CommAgent {

    private static final String PROPERTY_PORT = "PORT";
    public static final CommResource LOCAL_RESOURCE;
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);

    static {
        String localAgentName = COMPSsNode.getMasterName();
        String portStr = System.getProperty(COMPSsConstants.MASTER_PORT);
        int port = Integer.parseInt(portStr);
        if (localAgentName != null && localAgentName.isEmpty()) {
            localAgentName = null;
        }
        LOCAL_RESOURCE = new CommResource(localAgentName, port);
    }

    private CommAgent ownAgent;


    public CommAgentAdaptor() {
        super();
    }

    public CommAgentAdaptor(CommAgent ownAgent) {
        super();
        this.ownAgent = ownAgent;
    }

    @Override
    public Configuration constructConfiguration(Map<String, Object> projectProperties,
        Map<String, Object> resourcesProperties) throws ConstructConfigurationException {
        System.out.println("Constructing configuration");

        ResourcesExternalAdaptorProperties reaProp;
        reaProp = (ResourcesExternalAdaptorProperties) resourcesProperties.get("Properties");
        String port = "";

        for (ResourcesPropertyAdaptorType prop : reaProp.getProperty()) {
            String name = prop.getName();
            if (name.compareTo("Port") == 0) {
                port = prop.getValue();
            }
        }
        System.out.println("Crating Config with port " + port);
        MethodConfiguration mc = new MethodConfiguration(CommAgentAdaptor.class.getCanonicalName());
        mc.addProperty(PROPERTY_PORT, port);
        return mc;
    }

    @Override
    public COMPSsWorker initWorker(Configuration config, NodeMonitor monitor) {
        System.out.println("Initializing Worker ");
        MethodConfiguration aConf = (MethodConfiguration) config;

        String name = aConf.getHost();
        int port = Integer.parseInt(aConf.getProperty(PROPERTY_PORT));
        return new CommAgentWorker(name, port, this, monitor);
    }

    @Override
    public void receivedNewTask(NIONode master, NIOTask t, List<String> obsoleteData) {
        for (String obsolete : obsoleteData) {
            Comm.removeData(obsolete, true);
        }
        receivedNewTask(master, (CommTask) t);
    }

    @Override
    public void receivedNewTask(NIONode master, CommTask request) {
        ownAgent.receivedNewTask(master, request);
    }

    @Override
    public void print(Object o) {
        this.ownAgent.print(o);
    }

    @Override
    public void addResources(Resource<?, ?> res) {
        this.ownAgent.addResources(res);
    }

    @Override
    public void removeResources(Resource<?, ?> node) {
        this.ownAgent.removeResources(node);
    }

    @Override
    public void removeNode(String node) {
        this.ownAgent.removeNode(node);
    }

    @Override
    public void receivedRemoveObsoletes(NIONode node, List<String> obsolete) {
        for (String obsoleteData : obsolete) {
            Comm.removeData(obsoleteData, true);
        }
    }

    @Override
    public void lostNode(String node) {
        this.ownAgent.lostNode(node);
    }

    @Override
    public boolean isMyUuid(String uuid, String nodeName) {
        // This is used on NIOWorker to check sent UUID against worker UUID
        return true;
    }

    protected void retrieveAdditionalJobFiles(Connection connection, boolean success, int jobId, int taskId,
        JobHistory history) {
        // Agents do not retrieve information of how the job finished.
    }

    @Override
    protected void updateCopiedData(LogicalData tgtData, DataLocation actualLocation) {
        // Do nothing. ActualLocation could contain a proxy location, which is not currently supported.
    }

    // ownAgent needed as an interface to avoid circular dependencies
    @Override
    protected void finishJob(NIOJob nj, boolean successful, NIOTaskResult ntr, Exception e) {
        CommAgentJob commJob = (CommAgentJob) nj;
        commJob.taskFinished(successful, ntr, e, ownAgent);
    }

    // should not receive this call
    @Override
    public es.bsc.compss.types.resources.Resource getNodeFromResource(Resource<?, ?> r) {
        return null;
    }
}
