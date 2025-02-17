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

import es.bsc.compss.agent.RESTAgentConfig;
import es.bsc.compss.agent.rest.types.OrchestratorNotification;
import es.bsc.compss.agent.rest.types.RESTResult;
import es.bsc.compss.agent.rest.types.RemoteJobListener;
import es.bsc.compss.agent.rest.types.TaskProfile;
import es.bsc.compss.agent.rest.types.messages.StartApplicationRequest;
import es.bsc.compss.agent.util.RemoteJobsRegistry;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.types.job.JobHistory;
import es.bsc.compss.types.job.JobImpl;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Resource;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.Iterator;
import java.util.List;


public class RemoteRESTAgentJob extends JobImpl<RemoteRESTAgent> {

    private static final String REST_AGENT_URL =
        "http://" + COMPSsNode.getMasterName() + ":" + RESTAgentConfig.localAgentPort + "/";


    public RemoteRESTAgentJob(RemoteRESTAgent executor, int taskId, TaskDescription task, Implementation impl,
        Resource res, JobListener listener, List<Integer> predecessors, Integer numSuccessors) {
        super(taskId, task, impl, res, listener, predecessors, numSuccessors);
    }

    @Override
    public void submitJob() throws Exception {
        StartApplicationRequest sar = new StartApplicationRequest();

        RemoteRESTAgent executorNode = this.getResourceNode();

        WebTarget wt = executorNode.getTarget();
        wt = wt.path("/COMPSs/startApplication/");

        MethodDefinition mImpl = (MethodDefinition) this.impl.getDescription().getDefinition();
        // Get method definition properties
        String className = mImpl.getDeclaringClass();

        String methodName = mImpl.getAlternativeMethodName();
        if (methodName == null || methodName.isEmpty()) {
            methodName = taskParams.getName();
            mImpl.setAlternativeMethodName(taskParams.getName());
        }
        sar.setClassName(className);
        sar.setMethodName(methodName);
        sar.setCeiClass(null); // It is a task and we are not supporting Nested parallelism yet
        sar.setProlog(this.impl.getDescription().getProlog());
        sar.setEpilog(this.impl.getDescription().getEpilog());

        List<Parameter> params = taskParams.getParameters();
        int numParams = params.size();

        boolean hasReturn = taskParams.getNumReturns() > 0;
        boolean hasTarget = taskParams.hasTargetObject();

        if (hasReturn) {
            sar.setHasResult(true);
            numParams--;
        }

        if (hasTarget) {
            numParams--;
            Parameter param = params.get(numParams);
            DependencyParameter dPar = (DependencyParameter) param;
            DataAccessId faId = dPar.getDataAccessId();
            String renaming;
            if (faId.isWrite()) {
                renaming = ((DataAccessId.WritingDataAccessId) faId).getWrittenDataInstance().getRenaming();
            } else {
                renaming = ((DataAccessId.ReadingDataAccessId) faId).getReadDataInstance().getRenaming();
            }
            Object target = Comm.getData(renaming).getValue();
            System.out.println("Target: " + target);
            throw new UnsupportedOperationException("Instance methods not supported yet.");
        }

        System.out.println("SUBMISSION[" + this.getJobId() + "] Remote Agent :" + executorNode.getName());
        System.out.println("SUBMISSION[" + this.getJobId() + "] Parameters:");
        for (int parIdx = 0; parIdx < numParams; parIdx++) {
            System.out.println("SUBMISSION[" + this.getJobId() + "]     * Parameter " + parIdx + ": ");
            Parameter param = params.get(parIdx);
            DataType type = param.getType();
            System.out.println("SUBMISSION[" + this.getJobId() + "]         Type " + type);
            switch (type) {
                case FILE_T:
                case OBJECT_T:
                case STREAM_T:
                case EXTERNAL_STREAM_T:
                case EXTERNAL_PSCO_T:
                case PSCO_T:
                    DependencyParameter dPar = (DependencyParameter) param;
                    DataAccessId dAccId = dPar.getDataAccessId();
                    String inRenaming;
                    if (dAccId.isRead()) {
                        inRenaming = ((ReadingDataAccessId) dAccId).getReadDataInstance().getRenaming();
                    } else {
                        throw new JobExecutionException("Parameter" + Integer.toString(parIdx) + " is a Write access",
                            null);
                    }

                    if (inRenaming != null) {
                        String pscoId = Comm.getData(inRenaming).getPscoId();
                        if (pscoId != null) {
                            if (type.equals(DataType.OBJECT_T)) {
                                param.setType(DataType.PSCO_T);
                            }
                            // Change external object type
                            if (type.equals(DataType.FILE_T)) {
                                param.setType(DataType.EXTERNAL_PSCO_T);
                            }
                            type = param.getType();
                        }
                    }
                    if (type == DataType.PSCO_T || type == DataType.EXTERNAL_PSCO_T) {
                        Object value;
                        System.out.println("SUBMISSION[" + this.getJobId() + "]         Access " + dAccId);
                        value = dPar.getDataTarget();
                        System.out.println("SUBMISSION[" + this.getJobId() + "]         ID " + value);
                        sar.addPersistedParameter(param.getDirection(), (String) value);
                    } else {
                        throw new UnsupportedOperationException(
                            "Non-persisted DependencyParameters are not supported yet");
                    }
                    break;
                default:
                    BasicTypeParameter btParB = (BasicTypeParameter) param;
                    Object value = btParB.getValue();
                    System.out.println("SUBMISSION[" + this.getJobId() + "]         Value " + value);
                    sar.addParameter(value, btParB.getDirection(), btParB.getType(), btParB.getStream(),
                        btParB.getPrefix(), btParB.getName(), btParB.getContentType(), btParB.getWeight(),
                        btParB.isKeepRename());

            }
        }
        System.out.println("SUBMISSION[" + this.getJobId() + "] Stage in completed.");
        sar.setOrchestratorNotification(REST_AGENT_URL, OrchestratorNotification.HttpMethod.PUT,
            "COMPSs/endApplication/");

        Response response = wt.request(MediaType.APPLICATION_JSON).put(Entity.xml(sar), Response.class);

        if (response.getStatusInfo().getStatusCode() != 200) {
            System.out.println(response.readEntity(String.class));
            failed(JobEndStatus.SUBMISSION_FAILED);
        } else {
            System.out.println("SUBMISSION[" + this.getJobId() + "] Job submitted.");
            String jobId = response.readEntity(String.class);
            RemoteJobsRegistry.registerJobListener(jobId, new RemoteJobListener() {

                @Override
                public void finishedExecution(JobEndStatus endStatus, RESTResult[] results, TaskProfile profile) {
                    RemoteRESTAgentJob.this.executionStartedAt(profile.getExecutionStart());
                    RemoteRESTAgentJob.this.executionEndsAt(profile.getExecutionEnd());
                    System.out.println("SUBMISSION[" + getJobId() + "] Job completed.");
                    RemoteRESTAgentJob.this.taskFinished(endStatus, results);
                }
            });

        }
    }

    @Override
    public void cancelJob() throws Exception {

    }

    private void taskFinished(JobEndStatus endStatus, RESTResult[] results) {
        if (this.history == JobHistory.CANCELLED) {
            LOGGER.error("Ignoring notification since the job was cancelled");
            removeTmpData();
            return;
        }

        if (endStatus == JobEndStatus.OK) {
            RemoteRESTAgentJob.this.completed(results);
        } else {
            RemoteRESTAgentJob.this.failed(results, endStatus);
        }
    }

    private void completed(RESTResult[] results) {
        registerAllJobOutputs(results);
        super.completed();
    }

    private void failed(RESTResult[] results, JobEndStatus status) {
        if (this.isBeingCancelled()) {
            registerAllJobOutputs(results);
        } else {
            switch (this.taskParams.getOnFailure()) {
                case IGNORE:
                case CANCEL_SUCCESSORS:
                    registerAllJobOutputs(results);
                    break;
                default:
                    // case RETRY:
                    // case FAIL:
                    removeTmpData();
            }
        }
        super.failed(status);
    }

    private void registerAllJobOutputs(RESTResult[] results) {
        // Update information
        List<Parameter> taskParams = getTaskParams().getParameters();
        Iterator<Parameter> taskParamsItr = taskParams.iterator();
        int i = 0;
        while (taskParamsItr.hasNext()) {
            Parameter param = taskParamsItr.next();
            RESTResult result = results[i++];
            if (result != null) {
                registerParameter(param, result);
            }
        }
    }

    private void registerParameter(Parameter param, RESTResult result) {
        if (!param.isPotentialDependency()) {
            return;
        }

        DependencyParameter dp = (DependencyParameter) param;
        String rename = getOutputRename(dp);

        if (rename != null) {
            for (String rloc : result.getLocations()) {
                registerResultPrivateLocation(rloc, rename, this.worker);
            }
            notifyResultAvailability(dp, rename);
        }
    }

    @Override
    public TaskType getType() {
        return TaskType.METHOD;
    }

    @Override
    public String toString() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
