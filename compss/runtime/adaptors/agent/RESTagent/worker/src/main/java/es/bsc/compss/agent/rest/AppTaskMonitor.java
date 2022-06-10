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
package es.bsc.compss.agent.rest;

import es.bsc.compss.agent.AppMonitor;
import es.bsc.compss.agent.rest.types.OrchestratorNotification;
import es.bsc.compss.agent.rest.types.RESTAgentRequestHandler;
import es.bsc.compss.agent.rest.types.RESTAgentRequestListener;
import es.bsc.compss.agent.rest.types.TaskProfile;
import es.bsc.compss.agent.rest.types.messages.EndApplicationNotification;
import es.bsc.compss.agent.types.ApplicationParameter;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.worker.COMPSsException;

import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.jersey.client.ClientConfig;


/**
 * Class handling the status changes for a task and the corresponding notifications to its orchestrator.
 */
public class AppTaskMonitor extends AppMonitor implements RESTAgentRequestHandler {

    private static final Client CLIENT = ClientBuilder.newClient(new ClientConfig());

    private final RESTAgent owner;
    private final RESTAgentRequestListener requestListener;

    private boolean successful;

    private final TaskProfile profile;

    private static final Logger LOGGER = LogManager.getLogger(Loggers.AGENT);


    /**
     * Constructs a new AppTaskMonitor.
     *
     * @param args Monitored execution's arguments
     * @param target Monitored execution's target
     * @param results Monitored execution's results
     * @param owner RESTAgent handling the request
     * @param requestListener handler to notify the final state of the app task execution
     */
    public AppTaskMonitor(ApplicationParameter[] args, ApplicationParameter target, ApplicationParameter[] results,
        RESTAgent owner, RESTAgentRequestListener requestListener) {
        super(args, target, results);
        this.requestListener = requestListener;
        this.successful = false;
        this.profile = new TaskProfile();
        this.owner = owner;
    }

    @Override
    public void onCreation() {
        super.onCreation();
        profile.setTaskCreated(System.currentTimeMillis());
    }

    @Override
    public void onAccessesProcessed() {
        super.onAccessesProcessed();
        profile.setTaskAnalyzed(System.currentTimeMillis());
    }

    @Override
    public void onSchedule() {
        super.onSchedule();
        profile.setTaskScheduled(System.currentTimeMillis());
    }

    @Override
    public void onSubmission() {
        super.onSubmission();
        profile.setTaskSubmitted(System.currentTimeMillis());
    }

    @Override
    public void onDataReception() {
        super.onDataReception();
    }

    @Override
    public void valueGenerated(int paramId, TaskResult result) {
        super.valueGenerated(paramId, result);
        /*
         * this.paramTypes[paramId] = paramType; if (paramType == DataType.OBJECT_T) { LogicalData ld =
         * Comm.getData(dataId); StubItf psco = (StubItf) ld.getValue(); psco.makePersistent(ld.getName());
         * this.paramTypes[paramId] = DataType.PSCO_T; ld.setPscoId(psco.getID()); DataLocation outLoc = null; try {
         * SimpleURI targetURI = new SimpleURI(ProtocolType.PERSISTENT_URI.getSchema() + psco.getID()); outLoc =
         * DataLocation.createLocation(Comm.getAppHost(), targetURI); this.paramLocations[paramId] = outLoc.toString();
         * } catch (Exception e) { ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + dataId, e); } } else {
         * this.paramLocations[paramId] = dataLocation.toString(); }
         */
    }

    @Override
    public void onExecutionStart() {
        profile.setExecutionStart(System.currentTimeMillis());
    }

    @Override
    public void onExecutionStartAt(long ts) {
        profile.setExecutionStart(ts);
    }

    @Override
    public void onExecutionEnd() {
        profile.setExecutionEnd(System.currentTimeMillis());
    }

    @Override
    public void onExecutionEndAt(long ts) {
        profile.setExecutionEnd(ts);
    }

    @Override
    public void onAbortedExecution() {
        super.onAbortedExecution();
        profile.setEndNotification(System.currentTimeMillis());
    }

    @Override
    public void onErrorExecution() {
        super.onAbortedExecution();
        profile.setEndNotification(System.currentTimeMillis());
    }

    @Override
    public void onFailedExecution() {
        super.onFailedExecution();
        profile.setEndNotification(System.currentTimeMillis());
        this.successful = false;
    }

    @Override
    public void onException(COMPSsException e) {
        super.onException(e);
        profile.setEndNotification(System.currentTimeMillis());
    }

    @Override
    public void onSuccesfulExecution() {
        super.onSuccesfulExecution();
        profile.setEndNotification(System.currentTimeMillis());
        this.successful = true;
    }

    @Override
    public void onCancellation() {
        super.onCancellation();
        profile.setTaskEnd(System.currentTimeMillis());
        if (this.requestListener != null) {
            this.requestListener.requestCompleted(this);
        }
        System.out.println("Job cancelled after " + profile.getTotalTime());
    }

    @Override
    public void onCompletion() {
        super.onCompletion();
        profile.setTaskEnd(System.currentTimeMillis());
        if (this.requestListener != null) {
            this.requestListener.requestCompleted(this);
        }
        System.out.println("Job completed after " + profile.getTotalTime());
    }

    @Override
    public void onFailure() {
        super.onFailure();
        profile.setTaskEnd(System.currentTimeMillis());
        if (this.requestListener != null) {
            this.requestListener.requestCompleted(this);
        }
        System.out.println("Job failed after " + profile.getTotalTime());
    }

    @Override
    public void notifyOrchestrator(String host, OrchestratorNotification.HttpMethod method, String operation) {
        WebTarget target = CLIENT.target(host);
        WebTarget wt = target.path(operation);

        TaskResult[] results = this.getResults();
        DataType[] paramTypes = new DataType[results.length];
        String[] paramLocations = new String[results.length];
        int i = 0;
        for (TaskResult result : results) {
            paramTypes[i] = result.getType();
            paramLocations[i] = result.getDataLocation();
            i++;
        }
        EndApplicationNotification ean = new EndApplicationNotification("" + getAppId(),
            this.successful ? JobEndStatus.OK : JobEndStatus.EXECUTION_FAILED, paramTypes, paramLocations, profile);

        Response response = wt.request(MediaType.APPLICATION_JSON).put(Entity.xml(ean), Response.class);
        if (response.getStatusInfo().getStatusCode() != 200) {
            ErrorManager.warn("AGENT Could not notify Application " + getAppId() + " end to " + wt);
        }
    }

    @Override
    public void powerOff(List<String> forwardToHosts) {
        LOGGER.debug("AppTaskRequest completion initiates agent shutdown");
        if (forwardToHosts != null) {
            for (String host : forwardToHosts) {
                WebTarget target = CLIENT.target(host);
                LOGGER.debug("Forwarding stop action to: " + (target != null ? target.toString() : "null"));
                WebTarget wt = target.path("COMPSs/");
                Response response = wt.request().delete(Response.class);
                if (response.getStatusInfo().getStatusCode() != 200) {
                    ErrorManager.warn("AGENT Could not forward stop action to " + wt + ", returned code: "
                        + response.getStatusInfo().getStatusCode());
                }
            }
        }
        /*
         * A new Thread is necessary since poweroff is executed by the AccessProcessor thread. Stopping the agent is
         * sinchronous and waits for the whole COMPSs runtime to stop; therefore, the AccessProcessor waits for himself
         * to be stopped and cannot process the StopAP request.
         */
        new Thread() {

            @Override
            public void run() {
                AppTaskMonitor.this.owner.powerOff();
            }
        }.start();
    }
}
