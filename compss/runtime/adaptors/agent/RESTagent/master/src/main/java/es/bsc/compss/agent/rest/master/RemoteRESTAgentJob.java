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
package es.bsc.compss.agent.rest.master;

import es.bsc.compss.agent.RESTAgentConfig;
import es.bsc.compss.agent.rest.types.OrchestratorNotification;
import es.bsc.compss.agent.rest.types.RemoteJobListener;
import es.bsc.compss.agent.rest.types.TaskProfile;
import es.bsc.compss.agent.rest.types.messages.StartApplicationRequest;
import es.bsc.compss.agent.util.RemoteJobsRegistry;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobEndStatus;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.uri.SimpleURI;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


public class RemoteRESTAgentJob extends Job<RemoteRESTAgent> {

    private static final String REST_AGENT_URL =
        "http://" + COMPSsNode.getMasterName() + ":" + RESTAgentConfig.localAgentPort + "/";


    public RemoteRESTAgentJob(RemoteRESTAgent executor, int taskId, TaskDescription task, Implementation impl,
        Resource res, JobListener listener, List<Integer> predecessors, Integer numSuccessors) {
        super(taskId, task, impl, res, listener, predecessors, numSuccessors);
    }

    @Override
    public void submit() throws Exception {
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
            if (faId instanceof WAccessId) {
                // Write mode
                WAccessId waId = (WAccessId) faId;
                renaming = waId.getWrittenDataInstance().getRenaming();
            } else {
                if (faId instanceof RWAccessId) {
                    // Read write mode
                    RWAccessId rwaId = (RWAccessId) faId;
                    renaming = rwaId.getWrittenDataInstance().getRenaming();
                } else {
                    // Read only mode
                    RAccessId raId = (RAccessId) faId;
                    renaming = raId.getReadDataInstance().getRenaming();
                }
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
                    // String outRenaming;
                    if (dAccId instanceof WAccessId) {
                        throw new JobExecutionException("Target parameter is a Write access", null);
                    } else {
                        if (dAccId instanceof RWAccessId) {
                            // Read write mode
                            RWAccessId rwaId = (RWAccessId) dAccId;
                            inRenaming = rwaId.getReadDataInstance().getRenaming();
                            // outRenaming = rwaId.getWrittenDataInstance().getRenaming();
                        } else {
                            // Read only mode
                            RAccessId raId = (RAccessId) dAccId;
                            inRenaming = raId.getReadDataInstance().getRenaming();
                            // outRenaming = inRenaming;
                        }
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
                public void finishedExecution(JobEndStatus endStatus, DataType[] paramTypes, String[] paramLocations,
                    TaskProfile profile) {
                    RemoteRESTAgentJob.this.executionStartedAt(profile.getExecutionStart());
                    RemoteRESTAgentJob.this.executionEndsAt(profile.getExecutionEnd());
                    System.out.println("SUBMISSION[" + getJobId() + "] Job completed.");
                    stageout(paramTypes, paramLocations);
                    if (endStatus == JobEndStatus.OK) {
                        RemoteRESTAgentJob.this.completed();
                    } else {
                        RemoteRESTAgentJob.this.failed(endStatus);
                    }
                }
            });

        }
    }

    @Override
    public void cancelJob() throws Exception {
    }

    private void stageout(DataType[] paramTypes, String[] paramLocations) {
        List<Parameter> params = taskParams.getParameters();
        int numParams = params.size();

        boolean hasReturn = taskParams.getNumReturns() > 0;
        boolean hasTarget = taskParams.hasTargetObject();

        if (hasReturn) {
            numParams--;
            DependencyParameter returnParameter = (DependencyParameter) taskParams.getParameters().get(numParams);
            DataType type = paramTypes[numParams];
            String locString = paramLocations[numParams];
            System.out
                .println("STAGE OUT[" + this.getJobId() + "]         * Return type: " + type + " Value: " + locString);

            if (locString != null) {
                SimpleURI uri = new SimpleURI(locString);
                try {
                    DataLocation loc = DataLocation.createLocation(worker, uri);
                    if (loc.getProtocol() == ProtocolType.PERSISTENT_URI) {
                        type = returnParameter.getType();
                        if (type == DataType.OBJECT_T) {
                            type = DataType.PSCO_T;
                        }
                        returnParameter.setType(type);
                        String pscoId = loc.getLocationKey();
                        returnParameter.setDataTarget(pscoId);
                        System.out.println("STAGE OUT[" + this.getJobId() + "]         * Return : ");
                        System.out.println("STAGE OUT[" + this.getJobId() + "]             Type: " + type);
                        System.out.println("STAGE OUT[" + this.getJobId() + "]             ID: " + pscoId);
                    } else {
                        returnParameter.setType(type);
                        System.out.println("STAGE OUT[" + this.getJobId() + "]         * Return : ");
                        System.out.println("STAGE OUT[" + this.getJobId() + "]             Type: " + type);
                        System.out.println("STAGE OUT[" + this.getJobId() + "]             Value location: " + loc);

                    }
                } catch (IOException ioe) {
                    System.err.println("STAGE OUT[" + this.getJobId() + "] ERROR PROCESSING TASK RESULT");
                }
            }
        }

        if (hasTarget) {
            numParams--;
            DependencyParameter targetParameter = (DependencyParameter) taskParams.getParameters().get(numParams);
            DataType type = paramTypes[numParams];
            String locString = paramLocations[numParams];
            if (locString != null) {
                SimpleURI uri = new SimpleURI(locString);
                try {
                    DataLocation loc = DataLocation.createLocation(worker, uri);
                    if (loc.getProtocol() == ProtocolType.PERSISTENT_URI) {
                        type = targetParameter.getType();
                        if (type == DataType.OBJECT_T) {
                            type = DataType.PSCO_T;
                        }
                        targetParameter.setType(type);
                        String pscoId = loc.getLocationKey();
                        targetParameter.setDataTarget(pscoId);
                        System.out.println("STAGE OUT[" + this.getJobId() + "]         * Target : ");
                        System.out.println("STAGE OUT[" + this.getJobId() + "]             Type: " + type);
                        System.out.println("STAGE OUT[" + this.getJobId() + "]             ID: " + pscoId);
                    } else {
                        targetParameter.setType(type);
                        System.out.println("STAGE OUT[" + this.getJobId() + "]         * Target : ");
                        System.out.println("STAGE OUT[" + this.getJobId() + "]             Type: " + type);
                        System.out.println("STAGE OUT[" + this.getJobId() + "]             Value location: " + loc);

                    }
                } catch (IOException ioe) {
                    System.err.println("STAGE OUT[" + this.getJobId() + "] ERROR PROCESSING TASK TARGET");
                }
            }
        }

        System.out.println("STAGE OUT[" + this.getJobId() + "]     Parameters:");
        for (int parIdx = 0; parIdx < numParams; parIdx++) {
            DataType type = paramTypes[parIdx];
            switch (type) {
                case FILE_T:
                case EXTERNAL_PSCO_T:
                case OBJECT_T:
                case PSCO_T:
                    DependencyParameter dp = (DependencyParameter) params.get(parIdx);
                    String locString = paramLocations[parIdx];
                    if (locString != null) {
                        SimpleURI uri = new SimpleURI(locString);
                        try {
                            DataLocation loc = DataLocation.createLocation(worker, uri);
                            if (loc.getProtocol() == ProtocolType.PERSISTENT_URI) {
                                String pscoId = loc.getLocationKey();
                                switch (type) {
                                    case FILE_T:
                                        type = DataType.EXTERNAL_PSCO_T;
                                        break;
                                    case OBJECT_T:
                                        type = DataType.PSCO_T;
                                        break;
                                    default:
                                        // Nothing to do
                                        break;
                                }
                                dp.setType(type);
                                dp.setDataTarget(pscoId);
                                System.out
                                    .println("STAGE OUT[" + this.getJobId() + "]         * Parameter " + parIdx + ": ");
                                System.out.println("STAGE OUT[" + this.getJobId() + "]             Type: " + type);
                                System.out.println("STAGE OUT[" + this.getJobId() + "]             ID: " + pscoId);
                            } else {
                                switch (type) {
                                    case EXTERNAL_PSCO_T:
                                        type = DataType.FILE_T;
                                        break;
                                    case PSCO_T:
                                        type = DataType.OBJECT_T;
                                        break;
                                    default:
                                }
                                dp.setType(type);
                                System.out.println(
                                    "STAGE OUT[" + this.getJobId() + "]          * Parameter " + parIdx + ": ");
                                System.out.println("STAGE OUT[" + this.getJobId() + "]              Type: " + type);
                                System.out
                                    .println("STAGE OUT[" + this.getJobId() + "]              Value location: " + loc);
                            }
                        } catch (IOException ioe) {
                            System.err.println(
                                "STAGE OUT[" + this.getJobId() + "] ERROR PROCESSING TASK PARAMETER " + parIdx);
                        }
                    }
                    break;
                default:
            }
        }

    }

    @Override
    public String getHostName() {
        return getResourceNode().getName();
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
