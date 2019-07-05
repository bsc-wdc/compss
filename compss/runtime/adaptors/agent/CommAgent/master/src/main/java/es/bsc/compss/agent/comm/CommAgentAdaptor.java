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
package es.bsc.compss.agent.comm;

import es.bsc.comm.nio.NIONode;
import es.bsc.compss.agent.comm.messages.types.CommTask;
import es.bsc.compss.agent.types.Resource;
import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.master.NIOAdaptor;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;
import es.bsc.compss.types.resources.jaxb.ResourcesExternalAdaptorProperties;
import es.bsc.compss.types.resources.jaxb.ResourcesPropertyAdaptorType;
import java.util.List;
import java.util.Map;


/**
 * Class handling all the Adaptor requests to manage Comm Agent Workers.
 */
public class CommAgentAdaptor extends NIOAdaptor implements CommAgent {

    private static final String PROPERTY_PORT = "PORT";

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
    public COMPSsWorker initWorker(Configuration config) {
        System.out.println("Initializing Worker ");
        MethodConfiguration aConf = (MethodConfiguration) config;

        String name = aConf.getHost();
        int port = Integer.parseInt(aConf.getProperty(PROPERTY_PORT));
        return new CommAgentWorker(name, port, this);
    }

    @Override
    public void receivedNewTask(NIONode master, NIOTask t, List<String> obsoleteFiles) {
        // TODO: prune obsolete data
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
    public void lostNode(String node) {
        this.ownAgent.lostNode(node);
    }

}
