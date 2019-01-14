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
package es.bsc.compss.nio.master;

import java.util.LinkedList;
import java.util.List;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.commands.NIOData;

import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.RAccessId;
import es.bsc.compss.types.data.DataAccessId.RWAccessId;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.implementations.MultiNodeImplementation;
import es.bsc.compss.types.implementations.Implementation.TaskType;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.job.JobListener.JobEndStatus;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.DataAccessId.WAccessId;


public class NIOJob extends Job<NIOWorkerNode> {

    private final List<String> slaveWorkersNodeNames;


    public NIOJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res, List<String> slaveWorkersNodeNames,
            JobListener listener) {

        super(taskId, taskParams, impl, res, listener);
        this.slaveWorkersNodeNames = slaveWorkersNodeNames;
    }

    @Override
    public TaskType getType() {
        return TaskType.METHOD;
    }

    @Override
    public String getHostName() {
        return worker.getName();
    }

    @Override
    public void submit() throws Exception {
        // Prepare the job
        LOGGER.info("Submit NIOJob with ID " + jobId);
        NIOAdaptor.submitTask(this);
    }

    public NIOTask prepareJob() {
        AbstractMethodImplementation absMethodImpl = (AbstractMethodImplementation) this.impl;

        // If it is a native method, check that methodname is defined (otherwise define it from job parameters)
        // This is a workaround for Python
        switch (absMethodImpl.getMethodType()) {
            case METHOD:
                MethodImplementation methodImpl = (MethodImplementation) absMethodImpl;
                String methodName = methodImpl.getAlternativeMethodName();
                if (methodName == null || methodName.isEmpty()) {
                    methodImpl.setAlternativeMethodName(this.taskParams.getName());
                }
                break;
            case MULTI_NODE:
                MultiNodeImplementation multiNodeImpl = (MultiNodeImplementation) absMethodImpl;
                String multiNodeMethodName = multiNodeImpl.getMethodName();
                if (multiNodeMethodName == null || multiNodeMethodName.isEmpty()) {
                    multiNodeImpl.setMethodName(this.taskParams.getName());
                }
                break;
            default:
                // It is a non-native method, nothing to do
                break;
        }

        // Compute the task parameters
        LinkedList<NIOParam> params = addParams();
        int numParams = params.size() - taskParams.getNumReturns();

        // Create NIOTask
        NIOTask nt = new NIOTask(this.getLang(), DEBUG, absMethodImpl, taskParams.hasTargetObject(), taskParams.getNumReturns(), params,
                numParams, absMethodImpl.getRequirements(), this.slaveWorkersNodeNames, this.taskId, this.taskParams.getType(), this.jobId,
                this.history, this.transferId);

        return nt;
    }

    private LinkedList<NIOParam> addParams() {
        LinkedList<NIOParam> params = new LinkedList<>();
        for (Parameter param : taskParams.getParameters()) {
            DataType type = param.getType();
            NIOParam np;
            switch (type) {
                case FILE_T:
                case OBJECT_T:
                case PSCO_T:
                case EXTERNAL_PSCO_T:
                case BINDING_OBJECT_T:
                    DependencyParameter dPar = (DependencyParameter) param;
                    DataAccessId dAccId = dPar.getDataAccessId();
                    Object value = dPar.getDataTarget();
                    boolean preserveSourceData = true;
                    if (dAccId instanceof RAccessId) {
                        // Parameter is a R, has sources
                        preserveSourceData = ((RAccessId) dAccId).isPreserveSourceData();
                    } else if (dAccId instanceof RWAccessId) {
                        // Parameter is a RW, has sources
                        preserveSourceData = ((RWAccessId) dAccId).isPreserveSourceData();
                    } else {
                        // Parameter is a W, it has no sources
                        preserveSourceData = false;
                    }

                    // Check if the parameter has a valid PSCO and change its type
                    // OUT objects are restricted by the API
                    String renaming = null;
                    String dataMgmtId;
                    DataAccessId faId = dPar.getDataAccessId();
                    if (faId instanceof RWAccessId) {
                        // Read write mode
                        RWAccessId rwaId = (RWAccessId) faId;
                        renaming = rwaId.getReadDataInstance().getRenaming();
                        dataMgmtId = rwaId.getWrittenDataInstance().getRenaming();
                    } else if (faId instanceof RAccessId) {
                        // Read only mode
                        RAccessId raId = (RAccessId) faId;
                        renaming = raId.getReadDataInstance().getRenaming();
                        dataMgmtId = renaming;
                    } else {
                        WAccessId waId = (WAccessId) faId;
                        dataMgmtId = waId.getWrittenDataInstance().getRenaming();
                    }
                    if (renaming != null) {
                        String pscoId = Comm.getData(renaming).getPscoId();
                        if (pscoId != null) {
                            if (type.equals(DataType.OBJECT_T)) {
                                // Change Object type if it is a PSCO
                                param.setType(DataType.PSCO_T);
                            } else if (type.equals(DataType.FILE_T)) {
                                // Change external object type (Workaround for Python PSCO return objects)
                                param.setType(DataType.EXTERNAL_PSCO_T);
                            }
                            type = param.getType();
                        }
                    }

                    // Create the NIO Param
                    boolean writeFinalValue = !(dAccId instanceof RAccessId); // Only store W and RW
                    np = new NIOParam(dataMgmtId, type, param.getStream(), param.getPrefix(), param.getName(), preserveSourceData,
                            writeFinalValue, value, (NIOData) dPar.getDataSource(), dPar.getOriginalName());
                    break;

                default:
                    BasicTypeParameter btParB = (BasicTypeParameter) param;
                    value = btParB.getValue();
                    preserveSourceData = false; // Basic parameters are not preserved on Worker
                    writeFinalValue = false; // Basic parameters are not stored on Worker
                    np = new NIOParam(null, type, param.getStream(), param.getPrefix(), param.getName(), preserveSourceData,
                            writeFinalValue, value, null, DependencyParameter.NO_NAME);
                    break;
            }

            params.add(np);
        }
        return params;
    }

    public void taskFinished(boolean successful) {
        if (successful) {
            listener.jobCompleted(this);
        } else {
            listener.jobFailed(this, JobEndStatus.EXECUTION_FAILED);
        }
    }

    @Override
    public void stop() throws Exception {
        // Do nothing
    }

    @Override
    public String toString() {
        MethodImplementation method = (MethodImplementation) this.impl;

        String className = method.getDeclaringClass();
        String methodName = taskParams.getName();

        return "NIOJob JobId" + this.jobId + " for method " + methodName + " at class " + className;
    }

}
