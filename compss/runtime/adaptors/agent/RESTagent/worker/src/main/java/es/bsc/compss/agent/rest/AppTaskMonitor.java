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
package es.bsc.compss.agent.rest;

import es.bsc.compss.agent.Agent.AppMonitor;
import es.bsc.compss.agent.rest.types.Orchestrator;
import es.bsc.compss.agent.rest.types.messages.EndApplicationNotification;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.job.JobListener.JobEndStatus;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.ClientConfig;
import storage.StubItf;


public class AppTaskMonitor extends AppMonitor {

    private static final Client client = ClientBuilder.newClient(new ClientConfig());

    private final Orchestrator orchestrator;
    private final DataType[] paramTypes;
    private final String[] paramLocations;
    private boolean successful;

    public AppTaskMonitor(int numParams, Orchestrator orchestrator) {
        super();
        this.orchestrator = orchestrator;
        this.successful = false;
        this.paramTypes = new DataType[numParams];
        this.paramLocations = new String[numParams];

    }

    @Override
    public void onCreation() {
    }

    @Override
    public void onAccessesProcessed() {
    }

    @Override
    public void onSchedule() {
    }

    @Override
    public void onSubmission() {
    }

    @Override
    public void valueGenerated(int paramId, DataType type, String name, Object value) {
        paramTypes[paramId] = type;
        if (type == DataType.OBJECT_T) {
            LogicalData ld = Comm.getData(name);
            StubItf psco = (StubItf) ld.getValue();
            psco.makePersistent(ld.getName());
            paramTypes[paramId] = DataType.PSCO_T;
            ld.setPscoId(psco.getID());
            DataLocation outLoc = null;
            try {
                SimpleURI targetURI = new SimpleURI(DataLocation.Protocol.PERSISTENT_URI.getSchema() + psco.getID());
                outLoc = DataLocation.createLocation(Comm.getAppHost(), targetURI);
                paramLocations[paramId] = outLoc.toString();
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + name, e);
            }
        } else {
            paramLocations[paramId] = value.toString();
        }
    }

    @Override
    public void onErrorExecution() {
    }

    @Override
    public void onFailedExecution() {
        successful = false;
    }

    @Override
    public void onSuccesfulExecution() {
        successful = true;
    }

    @Override
    public void onCompletion() {
        if (orchestrator != null) {
            String masterId = orchestrator.getHost();
            String operation = orchestrator.getOperation();
            WebTarget target = client.target(masterId);
            WebTarget wt = target.path(operation);
            EndApplicationNotification ean = new EndApplicationNotification(
                    "" + getAppId(),
                    successful ? JobEndStatus.OK : JobEndStatus.EXECUTION_FAILED,
                    paramTypes, paramLocations);

            Response response = wt
                    .request(MediaType.APPLICATION_JSON)
                    .put(Entity.xml(ean), Response.class);
            if (response.getStatusInfo().getStatusCode() != 200) {
                ErrorManager.warn("AGENT Could not notify Application " + getAppId() + " end to " + wt);
            }
        }
    }

    @Override
    public void onFailure() {

    }
}
