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

import es.bsc.compss.agent.comm.messages.types.CommParam;
import es.bsc.compss.agent.comm.messages.types.CommParamCollection;
import es.bsc.compss.agent.comm.messages.types.CommResult;
import es.bsc.compss.agent.comm.messages.types.CommTask;
import es.bsc.compss.agent.types.PrivateRemoteDataLocation;
import es.bsc.compss.agent.types.RemoteDataInformation;
import es.bsc.compss.agent.types.RemoteDataLocation;
import es.bsc.compss.agent.types.SharedRemoteDataLocation;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.nio.NIOData;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOResult;
import es.bsc.compss.nio.NIOResultCollection;
import es.bsc.compss.nio.NIOTaskResult;
import es.bsc.compss.nio.NIOUri;
import es.bsc.compss.nio.master.NIOJob;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.DataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.implementations.definition.MultiNodeDefinition;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.CollectiveParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.util.CoreManager;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


/**
 * Class containing all the handling to submitJob a task execution on a CommAgentWorker.
 */
class CommAgentJob extends NIOJob {

    private CommAgent ownAgent;


    public CommAgentJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res,
        List<String> slaveWorkersNodeNames, JobListener listener, List<Integer> predecessors, Integer numSuccessors) {
        super(taskId, taskParams, impl, res, slaveWorkersNodeNames, listener, predecessors, numSuccessors);
    }

    /**
     * Marks the task as finished with the given {@code successful} error code.
     *
     * @param successful {@code true} if the task has successfully finished, {@code false} otherwise.
     * @param ntr information referring the results of the task executed
     * @param e Exception arose during the task execution, {@literal null} if no exception was raised.
     */
    public void taskFinished(boolean successful, NIOTaskResult ntr, Exception e, CommAgent ownAgent) {
        this.ownAgent = ownAgent;
        super.taskFinished(successful, ntr, e);
    }

    @Override
    public CommTask createNIOTask() {
        AbstractMethodImplementation absMethodImpl = (AbstractMethodImplementation) this.impl;

        // If it is a native method, check that methodname is defined (otherwise define it from job parameters)
        // This is a workaround for Python
        switch (absMethodImpl.getMethodType()) {
            case METHOD:
                MethodDefinition methodImpl = (MethodDefinition) absMethodImpl.getDefinition();
                String methodName = methodImpl.getAlternativeMethodName();
                if (methodName == null || methodName.isEmpty()) {
                    methodImpl.setAlternativeMethodName(this.taskParams.getName());
                }
                break;
            case MULTI_NODE:
                MultiNodeDefinition multiNodeImpl = (MultiNodeDefinition) absMethodImpl.getDefinition();
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
        CoreElement ce = CoreManager.getCore(impl.getCoreId());
        String ceSignature = ce.getSignature();
        CommTask nt = new CommTask(this.getLang(), DEBUG, ceSignature, absMethodImpl,
            this.taskParams.getParallelismSource(), this.taskParams.hasTargetObject(), this.taskParams.getNumReturns(),
            params, slaveWorkersNodeNames, this.taskId, this.jobId, this.history, this.transferId, this.getOnFailure(),
            this.getTimeOut(), CommAgentAdaptor.LOCAL_RESOURCE);

        return nt;
    }

    private LinkedList<NIOParam> addParams() {
        LinkedList<NIOParam> params = new LinkedList<>();
        for (Parameter param : this.taskParams.getParameters()) {
            CommParam commParam = createCommParamFromParameter(param);
            params.add(commParam);
        }
        return params;
    }

    private static CommParam createCommParamFromParameter(Parameter param) {
        CommParam commParam;
        switch (param.getType()) {
            case FILE_T:
            case OBJECT_T:
            case PSCO_T:
            case STREAM_T:
            case EXTERNAL_STREAM_T:
            case EXTERNAL_PSCO_T:
            case BINDING_OBJECT_T:
                commParam = buildCommParamFromDependencyParameter((DependencyParameter) param);
                break;
            case COLLECTION_T:
                CommParam tmpCp = buildCommParamFromDependencyParameter((DependencyParameter) param);
                commParam = buildCommCollectionParamFromCommParam(tmpCp, param);
                break;
            case DICT_COLLECTION_T:
                throw new UnsupportedOperationException();
            // break;
            default:
                commParam = buildCommParamFromBasicParameter((BasicTypeParameter) param);
                break;
        }
        return commParam;
    }

    private static CommParam buildCommParamFromBasicParameter(BasicTypeParameter param) {
        DataType type = param.getType();
        Direction dir = param.getDirection();
        StdIOStream stdIOStream = param.getStream();
        String prefix = param.getPrefix();
        String name = param.getName();
        String pyType = param.getContentType();
        double weight = param.getWeight();
        boolean keepRename = param.isKeepRename();
        CommParam commParam =
            new CommParam(null, type, dir, stdIOStream, prefix, name, pyType, weight, keepRename, null);
        commParam.setValue(((BasicTypeParameter) param).getValue());
        return commParam;
    }

    private static CommParam buildCommParamFromDependencyParameter(DependencyParameter dPar) {

        String renaming = null;
        String dataMgmtId;
        DataAccessId dAccId = dPar.getDataAccessId();
        if (dAccId.isRead()) {
            renaming = ((ReadingDataAccessId) dAccId).getReadDataInstance().getRenaming();
        }
        if (dAccId.isWrite()) {
            dataMgmtId = ((WritingDataAccessId) dAccId).getWrittenDataInstance().getRenaming();
        } else {
            dataMgmtId = renaming;
        }
        LogicalData sourceDataLD = null;
        String pscoId = null;
        if (renaming != null) {
            sourceDataLD = Comm.getData(renaming);
            pscoId = sourceDataLD.getPscoId();
        }

        if (pscoId != null) {
            if (dPar.getType().equals(DataType.OBJECT_T)) {
                // Change Object type if it is a PSCO
                dPar.setType(DataType.PSCO_T);
            } else {
                if (dPar.getType().equals(DataType.FILE_T)) {
                    // Change external object type for Python PSCO return objects
                    dPar.setType(DataType.EXTERNAL_PSCO_T);
                }
            }
        }

        DataType type = dPar.getType();
        Direction dir = dPar.getDirection();
        StdIOStream stdIOStream = dPar.getStream();
        String prefix = dPar.getPrefix();
        String name = dPar.getName();
        String pyType = dPar.getContentType();
        double weight = dPar.getWeight();
        boolean keepRename = dPar.isKeepRename();
        CommParam commParam = new CommParam(dataMgmtId, type, dir, stdIOStream, prefix, name, pyType, weight,
            keepRename, dPar.getOriginalName());
        commParam.setValue(dPar.getOriginalName());
        CommData sourceData = (CommData) dPar.getDataSource();
        if (sourceData != null) {
            RemoteDataInformation remoteData = new RemoteDataInformation(renaming);
            for (RemoteDataLocation rdl : sourceData.getRemoteLocations()) {
                remoteData.addSource(rdl);
            }
            commParam.setRemoteData(remoteData);
        }

        return commParam;
    }

    private static CommParam buildCommCollectionParamFromCommParam(CommParam commPar, Parameter param) {

        CommParamCollection npc = new CommParamCollection(commPar);

        CollectiveParameter<Parameter> collParam = (CollectiveParameter) param;
        for (Parameter subParam : collParam.getElements()) {
            npc.addParameter(CommAgentJob.createCommParamFromParameter(subParam));
        }

        return npc;
    }

    @Override
    protected void registerResult(Parameter param, NIOResult result) {
        if (!param.isPotentialDependency()) {
            return;
        }
        DependencyParameter dp = (DependencyParameter) param;
        String rename = getOutputRename(dp);
        if (dp.isCollective()) {
            CollectiveParameter colParam = (CollectiveParameter) param;
            NIOResultCollection colResult = (NIOResultCollection) result;

            List<NIOResult> taskResults = colResult.getElements();
            List<Parameter> taskParams = colParam.getElements();
            Iterator<Parameter> taskParamsItr = taskParams.iterator();
            Iterator<NIOResult> taskResultItr = taskResults.iterator();

            while (taskParamsItr.hasNext()) {
                Parameter elemParam = taskParamsItr.next();
                NIOResult elemResult = taskResultItr.next();
                registerResult(elemParam, elemResult);
            }
        } else {
            if (rename != null) {
                CommResult commResult = (CommResult) result;
                Collection<RemoteDataLocation> dataLocations = commResult.getLocations();
                for (RemoteDataLocation location : dataLocations) {
                    if (location != null) {
                        if (location.getType() == RemoteDataLocation.Type.SHARED) {
                            registerSharedResult((SharedRemoteDataLocation) location, rename);
                        } else {
                            registerPrivateResult((PrivateRemoteDataLocation) location, rename);
                        }
                    }
                }
                notifyResultAvailability(dp, rename);
            }
        }
    }

    private void registerSharedResult(SharedRemoteDataLocation sLocation, String rename) {
        String diskName = sLocation.getDiskName();
        for (SharedRemoteDataLocation.Mountpoint mp : sLocation.getMountpoints()) {
            Resource w = ownAgent.getNodeFromResource(mp.getResource());
            if (w == null) {
                w = this.worker;
            }
            w.addSharedDisk(diskName, mp.getPath());
        }
        LOGGER.debug("Registering result " + rename + " on shared disk " + diskName);
        registerResultSharedLocation(sLocation.getDiskName(), sLocation.getPathOnDisk(), rename);
    }

    private void registerPrivateResult(PrivateRemoteDataLocation pLocation, String rename) {
        Resource w = ownAgent.getNodeFromResource(pLocation.getResource());
        if (w == null) {
            w = this.worker;
        }
        LOGGER.debug("Registering result " + rename + " comming from worker " + w.getName());
        registerResultPrivateLocation(pLocation.getPath(), rename, w);
    }
}
