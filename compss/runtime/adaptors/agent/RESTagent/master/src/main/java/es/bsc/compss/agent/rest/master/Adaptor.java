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
package es.bsc.compss.agent.rest.master;

import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.resources.jaxb.ExternalAdaptorProperties;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.types.resources.jaxb.PropertyAdaptorType;
import es.bsc.compss.types.uri.MultiURI;
import java.util.LinkedList;
import java.util.List;


public class Adaptor implements CommAdaptor {

    @Override
    public void init() {

    }

    @Override
    public Configuration constructConfiguration(Object project_properties, Object resources_properties) throws ConstructConfigurationException {

        AgentConfiguration ac = new AgentConfiguration(this.getClass().getName());

        ExternalAdaptorProperties eap = (ExternalAdaptorProperties) resources_properties;
        for (PropertyAdaptorType prop : eap.getProperty()) {
            ac.addProperty(prop.getName(), prop.getValue());
        }
        /*ac.setHost((String) props.get("host"));
        MethodResourceDescription mrd = (MethodResourceDescription) props.get("description");
        ac.setDescription(mrd);
        ac.setLimitOfTasks(mrd.getTotalCPUComputingUnits());*/
        return ac;
    }

    @Override
    public COMPSsWorker initWorker(String workerName, Configuration config) {
        AgentConfiguration ac = (AgentConfiguration) config;
        return new RemoteRESTAgent(workerName, ac);
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

}
