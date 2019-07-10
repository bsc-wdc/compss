/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.comm.Connection;
import es.bsc.comm.TransferManager;
import es.bsc.comm.nio.NIONode;
import es.bsc.compss.agent.comm.messages.types.CommResource;
import es.bsc.compss.agent.comm.messages.types.CommTask;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOTaskResult;
import es.bsc.compss.nio.commands.CommandNIOTaskDone;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.uri.SimpleURI;
import java.util.LinkedList;


/**
 * Monitor to detect changes on a task state and notify the orchestrator that commanded its execution.
 */
class TaskMonitor extends PrintMonitor {

    private static final TransferManager TM = CommAgentAdaptor.getTransferManager();
    private final CommResource orchestrator;
    private final CommTask task;
    private final DataType[] paramTypes;
    private final String[] paramLocations;

    private boolean successful;

    public TaskMonitor(CommResource orchestrator, CommTask request) {
        this.orchestrator = orchestrator;
        this.successful = false;
        this.task = request;
        int argsCount = request.getParams().size();
        int resultsCount = request.getResults().size();
        int numParams = argsCount + resultsCount + (request.getTarget() != null ? 1 : 0);
        this.paramTypes = new DataType[numParams];
        this.paramLocations = new String[numParams];
    }

    @Override
    public void valueGenerated(int paramId, String paramName, DataType paramType, String dataId, Object dataLocation) {
        super.valueGenerated(paramId, paramName, paramType, dataId, dataLocation);
        this.paramTypes[paramId] = paramType;
        if (paramType == DataType.OBJECT_T) {
            LogicalData ld = Comm.getData(dataId);
            if (ld.getPscoId() != null) {
                this.paramTypes[paramId] = DataType.PSCO_T;
                SimpleURI targetURI = new SimpleURI(ProtocolType.PERSISTENT_URI.getSchema() + ld.getPscoId());
                this.paramLocations[paramId] = targetURI.toString();
            } else {
                this.paramTypes[paramId] = paramType;
                this.paramLocations[paramId] = dataLocation.toString();
            }
        } else {
            this.paramTypes[paramId] = paramType;
            this.paramLocations[paramId] = dataLocation.toString();
        }
        NIOParam originalParam = obtainJobParameter(paramId);
        String originalDataMgmtId = originalParam.getDataMgmtId();
        if (dataId.compareTo(originalDataMgmtId) != 0) {
            Comm.linkData(originalDataMgmtId, dataId);
        }
    }

    private NIOParam obtainJobParameter(int paramId) {
        int paramsCount = task.getParams().size();
        if (paramId < paramsCount) {
            return task.getParams().get(paramId);
        }

        paramId -= paramsCount;
        if (task.getTarget() != null) {
            if (paramId == 0) {
                return task.getTarget();
            }
            paramId--;
        }
        return task.getResults().get(paramId);
    }

    @Override
    public void onSuccesfulExecution() {
        super.onSuccesfulExecution();
        this.successful = true;
    }

    @Override
    public void onCompletion() {
        super.onCompletion();
        if (this.orchestrator != null) {
            notifyEnd();
        }
    }

    @Override
    public void onFailure() {
        super.onFailure();
        if (this.orchestrator != null) {
            notifyEnd();
        }
    }

    public void notifyEnd() {
        NIONode n = new NIONode(orchestrator.getName(), orchestrator.getPort());
        CommandNIOTaskDone cmd = null;

        int jobId = task.getJobId();
        NIOTaskResult tr = new NIOTaskResult(jobId, new LinkedList<>(), null, new LinkedList<>());

        Connection c = TM.startConnection(n);
        cmd = new CommandNIOTaskDone(tr, successful, null);
        c.sendCommand(cmd);
        c.finishConnection();
    }
}
