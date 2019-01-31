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

import es.bsc.compss.agent.RESTAgentConstants;
import es.bsc.compss.agent.rest.types.Orchestrator;
import es.bsc.compss.agent.rest.types.RemoteJobListener;
import es.bsc.compss.agent.rest.types.messages.StartApplicationRequest;
import es.bsc.compss.agent.util.RemoteJobsRegistry;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.job.JobListener.JobEndStatus;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.uri.SimpleURI;
import java.io.IOException;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


public class RemoteRESTAgentJob extends Job<RemoteRESTAgent> {

    private static final String REST_AGENT_URL = "http://" + System.getProperty(RESTAgentConstants.COMPSS_AGENT_NAME) + ":" + System.getProperty(RESTAgentConstants.COMPSS_AGENT_PORT) + "/";
    private final RemoteRESTAgent executor;

    public RemoteRESTAgentJob(RemoteRESTAgent executor, int taskId, TaskDescription task, Implementation impl, Resource res, JobListener listener) {
        super(taskId, task, impl, res, listener);
        this.executor = executor;
    }

    @Override
    public void submit() throws Exception {
        StartApplicationRequest sar = new StartApplicationRequest();

        WebTarget wt = executor.getTarget();
        wt = wt.path("/COMPSs/startApplication/");

        MethodImplementation mImpl = (MethodImplementation) this.impl;
        // Get method definition properties
        String className = mImpl.getDeclaringClass();

        String methodName = mImpl.getAlternativeMethodName();
        if (methodName == null || methodName.isEmpty()) {
            methodName = taskParams.getName();
            mImpl.setAlternativeMethodName(taskParams.getName());
        }
        sar.setClassName(className);
        sar.setMethodName(methodName);
        sar.setCeiClass(null); //It is a task and we are not supporting Nested parallelism yet

        Parameter[] params = taskParams.getParameters();
        int numParams = params.length;

        boolean hasReturn = taskParams.getNumReturns() > 0;
        Object retValue = null;
        boolean hasTarget = taskParams.hasTargetObject();
        Object target = null;

        if (hasReturn) {
            sar.setHasResult(true);
            numParams--;
        }

        if (hasTarget) {
            numParams--;
            Parameter param = params[numParams];
            DependencyParameter dPar = (DependencyParameter) param;
            DataAccessId faId = dPar.getDataAccessId();
            String renaming;
            if (faId instanceof DataAccessId.WAccessId) {
                // Write mode
                DataAccessId.WAccessId waId = (DataAccessId.WAccessId) faId;
                renaming = waId.getWrittenDataInstance().getRenaming();
            } else if (faId instanceof DataAccessId.RWAccessId) {
                // Read write mode
                DataAccessId.RWAccessId rwaId = (DataAccessId.RWAccessId) faId;
                renaming = rwaId.getWrittenDataInstance().getRenaming();
            } else {
                // Read only mode
                DataAccessId.RAccessId raId = (DataAccessId.RAccessId) faId;
                renaming = raId.getReadDataInstance().getRenaming();
            }
            target = Comm.getData(renaming).getValue();
            throw new UnsupportedOperationException("Instance methods not supported yet.");
        }

        System.out.println("SUBMISSION[" + this.getJobId() + "] Remote Agent :" + executor.getName());
        System.out.println("SUBMISSION[" + this.getJobId() + "] Parameters:");
        for (int parIdx = 0; parIdx < numParams; parIdx++) {
            System.out.println("SUBMISSION[" + this.getJobId() + "]     * Parameter " + parIdx + ": ");
            Parameter param = params[parIdx];
            DataType type = param.getType();
            System.out.println("SUBMISSION[" + this.getJobId() + "]         Type " + type);
            switch (type) {
                case FILE_T:
                case OBJECT_T:
                case EXTERNAL_PSCO_T:
                case PSCO_T:
                    DependencyParameter dPar = (DependencyParameter) param;
                    DataAccessId dAccId = dPar.getDataAccessId();
                    String inRenaming;
                    String renaming;
                    if (dAccId instanceof DataAccessId.WAccessId) {
                        throw new JobExecutionException("Target parameter is a Write access", null);
                    } else if (dAccId instanceof DataAccessId.RWAccessId) {
                        // Read write mode
                        DataAccessId.RWAccessId rwaId = (DataAccessId.RWAccessId) dAccId;
                        inRenaming = rwaId.getReadDataInstance().getRenaming();
                        renaming = rwaId.getWrittenDataInstance().getRenaming();
                    } else {
                        // Read only mode
                        DataAccessId.RAccessId raId = (DataAccessId.RAccessId) dAccId;
                        inRenaming = raId.getReadDataInstance().getRenaming();
                        renaming = inRenaming;
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
                        sar.addPersistedParameter((String) value, param.getDirection());
                    } else {
                        throw new UnsupportedOperationException("Non-persisted DependencyParameters are not supported yet");
                    }
                    break;
                default:
                    BasicTypeParameter btParB = (BasicTypeParameter) param;
                    Object value = btParB.getValue();
                    System.out.println("SUBMISSION[" + this.getJobId() + "]         Value " + value);
                    sar.addParameter(btParB, value);
            }
        }
        System.out.println("SUBMISSION[" + this.getJobId() + "] Stage in completed.");
        sar.setOrchestrator(REST_AGENT_URL, Orchestrator.HttpMethod.PUT, "COMPSs/endApplication/");

        Response response = wt
                .request(MediaType.APPLICATION_JSON)
                .put(Entity.xml(sar), Response.class);

        if (response.getStatusInfo().getStatusCode() != 200) {
            System.out.println(response.readEntity(String.class));
            this.getListener().jobFailed(this, JobListener.JobEndStatus.SUBMISSION_FAILED);
        } else {
            System.out.println("SUBMISSION[" + this.getJobId() + "] Job submitted.");
            String jobId = response.readEntity(String.class);
            RemoteJobsRegistry.registerJobListener(jobId, new RemoteJobListener() {
                @Override
                public void finishedExecution(JobListener.JobEndStatus endStatus, String[] paramResults) {
                    System.out.println("SUBMISSION[" + getJobId() + "] Job completed.");
                    stageout(paramResults);
                    if (endStatus == JobEndStatus.OK) {
                        getListener().jobCompleted(RemoteRESTAgentJob.this);
                    } else {
                        getListener().jobFailed(RemoteRESTAgentJob.this, endStatus);
                    }
                }
            });

        }
    }

    @Override
    public void stop() throws Exception {
    }

    private void stageout(String[] paramResults) {
        Parameter[] params = taskParams.getParameters();
        int numParams = params.length;

        boolean hasReturn = taskParams.getNumReturns() > 0;
        boolean hasTarget = taskParams.hasTargetObject();

        if (hasReturn) {
            numParams--;
            DependencyParameter returnParameter = (DependencyParameter) taskParams.getParameters()[numParams];
            DataType type = returnParameter.getType();
            String locString = paramResults[numParams];
            if (locString != null) {
                SimpleURI uri = new SimpleURI(locString);
                try {
                    DataLocation loc = DataLocation.createLocation(worker, uri);
                    if (loc.getProtocol() == DataLocation.Protocol.PERSISTENT_URI) {
                        String pscoId = loc.getLocationKey();
                        type = returnParameter.getType();
                        if (type == DataType.OBJECT_T) {
                            type = DataType.PSCO_T;
                        }
                        returnParameter.setType(type);
                        returnParameter.setDataTarget(pscoId);
                        System.out.println("STAGE OUT         * Return : ");
                        System.out.println("STAGE OUT             Type: " + type);
                        System.out.println("STAGE OUT             ID: " + pscoId);
                    } else {
                        returnParameter.setType(type);
                        System.out.println("STAGE OUT         * Return : ");
                        System.out.println("STAGE OUT             Type: " + type);
                        System.out.println("STAGE OUT             Value location: " + loc);

                    }
                } catch (IOException ioe) {
                    System.err.println("STAGE OUT ERROR PROCESSING TASK RESULT");
                }
            }
        }

        if (hasTarget) {
            numParams--;
            DependencyParameter targetParameter = (DependencyParameter) taskParams.getParameters()[numParams];
            DataType type = targetParameter.getType();
            String locString = paramResults[numParams];
            if (locString != null) {
                SimpleURI uri = new SimpleURI(locString);
                try {
                    DataLocation loc = DataLocation.createLocation(worker, uri);
                    if (loc.getProtocol() == DataLocation.Protocol.PERSISTENT_URI) {
                        String pscoId = loc.getLocationKey();
                        type = targetParameter.getType();
                        if (type == DataType.OBJECT_T) {
                            type = DataType.PSCO_T;
                        }
                        targetParameter.setType(type);
                        targetParameter.setDataTarget(pscoId);
                        System.out.println("STAGE OUT         * Return : ");
                        System.out.println("STAGE OUT             Type: " + type);
                        System.out.println("STAGE OUT             ID: " + pscoId);
                    } else {
                        targetParameter.setType(type);
                        System.out.println("STAGE OUT         * Return : ");
                        System.out.println("STAGE OUT             Type: " + type);
                        System.out.println("STAGE OUT             Value location: " + loc);

                    }
                } catch (IOException ioe) {
                    System.err.println("STAGE OUT ERROR PROCESSING TASK TARGET");
                }
            }
        }

        System.out.println("STAGE OUT     Parameters:");
        for (int parIdx = 0; parIdx < numParams; parIdx++) {
            Parameter param = params[parIdx];
            DataType type = param.getType();

            switch (type) {
                case FILE_T:
                case EXTERNAL_PSCO_T:
                case OBJECT_T:
                case PSCO_T:

                    DependencyParameter dp = (DependencyParameter) params[parIdx];
                    String locString = paramResults[parIdx];
                    if (locString != null) {
                        SimpleURI uri = new SimpleURI(locString);
                        try {
                            DataLocation loc = DataLocation.createLocation(worker, uri);
                            if (loc.getProtocol() == DataLocation.Protocol.PERSISTENT_URI) {
                                String pscoId = loc.getLocationKey();
                                switch (type) {
                                    case FILE_T:
                                        type = DataType.EXTERNAL_PSCO_T;
                                        break;
                                    case OBJECT_T:
                                        type = DataType.PSCO_T;
                                        break;
                                }
                                dp.setType(type);
                                dp.setDataTarget(pscoId);
                                System.out.println("STAGE OUT         * Parameter " + parIdx + ": ");
                                System.out.println("STAGE OUT             Type: " + type);
                                System.out.println("STAGE OUT             ID: " + pscoId);
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
                                System.out.println("STAGE OUT          * Parameter " + parIdx + ": ");
                                System.out.println("STAGE OUT              Type: " + type);
                                System.out.println("STAGE OUT              Value location: " + loc);
                            }
                        } catch (IOException ioe) {
                            System.err.println("STAGE OUT ERROR PROCESSING TASK PARAMETER " + parIdx);
                        }
                        break;
                    }
                default:
            }
        }

    }

    @Override
    public String getHostName() {
        return getResourceNode().getName();
    }

    @Override
    public Implementation.TaskType getType() {
        return Implementation.TaskType.METHOD;
    }

    @Override
    public String toString() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
