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

package es.bsc.compss.agent.comm;

import es.bsc.compss.agent.comm.messages.types.CommParam;
import es.bsc.compss.agent.comm.messages.types.CommParamCollection;
import es.bsc.compss.agent.comm.messages.types.CommTask;
import es.bsc.compss.agent.types.RemoteDataInformation;
import es.bsc.compss.agent.types.RemoteDataLocation;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.nio.NIOData;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOUri;
import es.bsc.compss.nio.master.NIOJob;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.implementations.definition.MultiNodeDefinition;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.CollectionParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.util.CoreManager;

import java.util.LinkedList;
import java.util.List;


/**
 * Class containing all the handling to submit a task execution on a CommAgentWorker.
 */
class CommAgentJob extends NIOJob {

    public CommAgentJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res,
        List<String> slaveWorkersNodeNames, JobListener listener, List<Integer> predecessors, Integer numSuccessors) {
        super(taskId, taskParams, impl, res, slaveWorkersNodeNames, listener, predecessors, numSuccessors);
    }

    @Override
    public CommTask prepareJob() {
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
            params, numParams, absMethodImpl.getRequirements(), slaveWorkersNodeNames, this.taskId,
            this.impl.getTaskType(), this.jobId, this.history, this.transferId, this.getOnFailure(), this.getTimeOut(),
            CommAgentAdaptor.LOCAL_RESOURCE);

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
        if (dAccId instanceof RWAccessId) {
            // Read write mode
            RWAccessId rwaId = (RWAccessId) dAccId;
            renaming = rwaId.getReadDataInstance().getRenaming();
            dataMgmtId = rwaId.getWrittenDataInstance().getRenaming();
        } else {
            if (dAccId instanceof RAccessId) {
                // Read only mode
                RAccessId raId = (RAccessId) dAccId;
                renaming = raId.getReadDataInstance().getRenaming();
                dataMgmtId = renaming;
            } else {
                WAccessId waId = (WAccessId) dAccId;
                dataMgmtId = waId.getWrittenDataInstance().getRenaming();
            }
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
        NIOData sourceData = (NIOData) dPar.getDataSource();
        if (sourceData != null) {
            RemoteDataInformation remoteData = new RemoteDataInformation(renaming);
            for (NIOUri uri : sourceData.getSources()) {
                if (uri instanceof CommAgentURI) {
                    CommAgentURI caURI = (CommAgentURI) uri;
                    remoteData.addSource(new RemoteDataLocation(caURI.getAgent(), uri.getPath()));
                } else {
                    CommAgentURI caURI = new CommAgentURI(uri);
                    remoteData.addSource(new RemoteDataLocation(caURI.getAgent(), uri.getPath()));
                }

            }

            commParam.setRemoteData(remoteData);
        }

        return commParam;
    }

    private static CommParam buildCommCollectionParamFromCommParam(CommParam commPar, Parameter param) {

        CommParamCollection npc = new CommParamCollection(commPar);

        CollectionParameter collParam = (CollectionParameter) param;
        for (Parameter subParam : collParam.getParameters()) {
            npc.addParameter(CommAgentJob.createCommParamFromParameter(subParam));
        }

        return npc;
    }
}
