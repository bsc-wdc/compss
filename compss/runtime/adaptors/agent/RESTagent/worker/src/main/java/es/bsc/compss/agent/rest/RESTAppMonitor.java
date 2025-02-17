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
package es.bsc.compss.agent.rest;

import es.bsc.compss.agent.AppMonitor;
import es.bsc.compss.agent.rest.types.OrchestratorNotification;
import es.bsc.compss.agent.rest.types.RESTAgentRequestHandler;
import es.bsc.compss.agent.rest.types.RESTAgentRequestListener;
import es.bsc.compss.agent.rest.types.RESTResult;
import es.bsc.compss.agent.rest.types.TaskProfile;
import es.bsc.compss.agent.rest.types.messages.EndApplicationNotification;
import es.bsc.compss.agent.types.ApplicationParameter;
import es.bsc.compss.agent.types.PrivateRemoteDataLocation;
import es.bsc.compss.agent.types.RemoteDataLocation;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.worker.COMPSsException;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.jersey.client.ClientConfig;


/**
 * Class handling the status changes for a task and the corresponding notifications to its orchestrator.
 */
public class RESTAppMonitor extends AppMonitor implements RESTAgentRequestHandler {

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
    public RESTAppMonitor(ApplicationParameter[] args, ApplicationParameter target, ApplicationParameter[] results,
        RESTAgent owner, RESTAgentRequestListener requestListener) {
        super(args, target, results);
        this.requestListener = requestListener;
        this.successful = false;
        this.profile = new TaskProfile();
        this.owner = owner;
    }

    @Override
    public UniqueTaskMonitor getTaskMonitor() {
        return new RESTTaskMonitor();
    }

    @Override
    public void specificOnCancellation() {
        if (RESTAppMonitor.this.requestListener != null) {
            RESTAppMonitor.this.requestListener.requestCompleted(RESTAppMonitor.this);
        }
        System.out.println("App cancelled after " + profile.getTotalTime());
    }

    @Override
    public void specificOnCompletion() {
        if (RESTAppMonitor.this.requestListener != null) {
            RESTAppMonitor.this.requestListener.requestCompleted(RESTAppMonitor.this);
        }
        System.out.println("App completed after " + profile.getTotalTime());
    }

    @Override
    public void specificOnException(COMPSsException e) {
        // Do nothing
    }

    @Override
    public void specificOnFailure() {
        if (RESTAppMonitor.this.requestListener != null) {
            RESTAppMonitor.this.requestListener.requestCompleted(RESTAppMonitor.this);
        }
        System.out.println("App failed after " + profile.getTotalTime());
    }

    @Override
    public void notifyOrchestrator(String host, OrchestratorNotification.HttpMethod method, String operation) {
        WebTarget target = CLIENT.target(host);
        WebTarget wt = target.path(operation);

        TaskResult[] taskResults = this.getResults();
        RESTResult[] restResults = new RESTResult[taskResults.length];
        int i = 0;
        for (TaskResult result : taskResults) {
            Collection<RemoteDataLocation> remoteLocations = result.getLocations();
            String[] locs = remoteDataInfoToStringArray(remoteLocations);
            restResults[i] = new RESTResult(locs);
            i++;
        }
        EndApplicationNotification ean = new EndApplicationNotification("" + getAppId(),
            this.successful ? JobEndStatus.OK : JobEndStatus.EXECUTION_FAILED, restResults, profile);

        Response response = wt.request(MediaType.APPLICATION_JSON).put(Entity.xml(ean), Response.class);
        if (response.getStatusInfo().getStatusCode() != 200) {
            ErrorManager.warn("AGENT Could not notify Application " + getAppId() + " end to " + wt);
        }
    }

    private String[] remoteDataInfoToStringArray(Collection<RemoteDataLocation> remoteLocations) {
        String[] res = new String[remoteLocations.size()];
        int i = 0;
        for (RemoteDataLocation remoteLoc : remoteLocations) {
            res[i] = ((PrivateRemoteDataLocation) remoteLoc).getPath();
            i++;
        }
        return res;
    }

    @Override
    public void powerOff(List<String> forwardToHosts) {
        /*
         * A new Thread is necessary since poweroff is executed by the AccessProcessor thread. Stopping the agent is
         * sinchronous and waits for the whole COMPSs runtime to stop; therefore, the AccessProcessor waits for himself
         * to be stopped and cannot process the StopAP request.
         */
        new Thread() {

            @Override
            public void run() {

                LOGGER.debug("AppTaskRequest completion initiates agent shutdown");
                if (forwardToHosts != null) {
                    final Semaphore sem = new Semaphore(0);
                    for (String host : forwardToHosts) {
                        new Thread() {

                            public void run() {
                                WebTarget target = CLIENT.target(host);
                                if (target != null) {
                                    LOGGER.debug("Forwarding stop action to: " + host);
                                    WebTarget wt = target.path("COMPSs/");
                                    Response response = wt.request().delete(Response.class);
                                    if (response.getStatusInfo().getStatusCode() != 200) {
                                        ErrorManager.warn("AGENT Could not forward stop action to " + wt
                                            + ", returned code: " + response.getStatusInfo().getStatusCode());
                                    }
                                    LOGGER.debug(host + " has been stopped");
                                } else {
                                    LOGGER.warn("Could not contact " + host + " to stop it");
                                }
                                sem.release();
                            }
                        }.start();
                    }
                    sem.acquireUninterruptibly(forwardToHosts.size());
                }

                RESTAppMonitor.this.owner.powerOff();
            }
        }.start();
    }


    protected class RESTTaskMonitor extends UniqueTaskMonitor {

        @Override
        public void specificOnCreation() {
            profile.setTaskCreated(System.currentTimeMillis());
        }

        @Override
        public void specificOnAccessesProcessed() {
            profile.setTaskAnalyzed(System.currentTimeMillis());
        }

        @Override
        public void specificOnSchedule() {
            profile.setTaskScheduled(System.currentTimeMillis());
        }

        @Override
        public void specificOnSubmission() {
            profile.setTaskSubmitted(System.currentTimeMillis());
        }

        @Override
        public void specificOnDataReception() {
        }

        @Override
        public void specificOnExecutionStart() {
            profile.setExecutionStart(System.currentTimeMillis());
        }

        @Override
        public void specificOnExecutionStartAt(long ts) {
            profile.setExecutionStart(ts);
        }

        @Override
        public void specificOnExecutionEnd() {
            profile.setExecutionEnd(System.currentTimeMillis());
        }

        @Override
        public void specificOnExecutionEndAt(long ts) {
            profile.setExecutionEnd(ts);
        }

        @Override
        public void specificOnAbortedExecution() {
            profile.setEndNotification(System.currentTimeMillis());
        }

        @Override
        public void specificOnErrorExecution() {
            profile.setEndNotification(System.currentTimeMillis());
        }

        @Override
        public void specificOnFailedExecution() {
            profile.setEndNotification(System.currentTimeMillis());
            RESTAppMonitor.this.successful = false;
        }

        @Override
        public void specificOnException(COMPSsException e) {
            profile.setEndNotification(System.currentTimeMillis());
        }

        @Override
        public void specificOnSuccessfulExecution() {
            profile.setEndNotification(System.currentTimeMillis());
            RESTAppMonitor.this.successful = true;
        }

        @Override
        public void specificOnCancellation() {
            profile.setTaskEnd(System.currentTimeMillis());
        }

        @Override
        public void specificOnCompletion() {
            profile.setTaskEnd(System.currentTimeMillis());
        }

        @Override
        public void specificOnFailure() {
            profile.setTaskEnd(System.currentTimeMillis());
        }
    }
}
