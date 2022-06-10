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

import es.bsc.comm.Connection;
import es.bsc.comm.TransferManager;
import es.bsc.comm.nio.NIONode;
import es.bsc.compss.agent.AppMonitor;
import es.bsc.compss.agent.comm.messages.types.CommResource;
import es.bsc.compss.agent.comm.messages.types.CommTask;
import es.bsc.compss.agent.types.ApplicationParameter;
import es.bsc.compss.nio.NIOTaskResult;
import es.bsc.compss.nio.commands.CommandDataReceived;
import es.bsc.compss.nio.commands.CommandNIOTaskDone;


/**
 * Monitor to detect changes on a task state and notify the orchestrator that commanded its execution.
 */
class TaskMonitor extends AppMonitor {

    private static final TransferManager TM = CommAgentAdaptor.getTransferManager();
    private final CommResource orchestrator;
    private final CommTask task;

    private boolean successful;


    /**
     * Constructs a new Task Monitor.
     *
     * @param args Monitored execution's arguments
     * @param target Monitored execution's target
     * @param results Monitored execution's results
     * @param orchestrator endpoint where to notify changes on the execution
     * @param request Execution request
     */
    public TaskMonitor(ApplicationParameter[] args, ApplicationParameter target, ApplicationParameter[] results,
        CommResource orchestrator, CommTask request) {
        super(args, target, results);
        this.orchestrator = orchestrator;
        this.successful = false;
        this.task = request;
        this.task.profileArrival();
    }

    @Override
    public void onDataReception() {
        super.onDataReception();
        this.task.profileFetchedData();
        if (this.orchestrator != null) {
            NIONode n = new NIONode(orchestrator.getName(), orchestrator.getPort());

            int transferGroupId = this.task.getTransferGroupId();

            Connection c = TM.startConnection(n);
            CommandDataReceived cmd = new CommandDataReceived(transferGroupId);
            c.sendCommand(cmd);
            c.finishConnection();
        }
    }

    @Override
    public void valueGenerated(int paramId, TaskResult param) {
        super.valueGenerated(paramId, param);
    }

    @Override
    public void onExecutionStart() {
        task.getProfile().executionStarts();
    }

    @Override
    public void onExecutionStartAt(long ts) {
        task.getProfile().executionStartsAt(ts);
    }

    @Override
    public void onExecutionEnd() {
        task.getProfile().executionEnds();
    }

    @Override
    public void onExecutionEndAt(long ts) {
        task.getProfile().executionEndsAt(ts);
    }

    @Override
    public void onSuccesfulExecution() {
        super.onSuccesfulExecution();
        this.successful = true;
    }

    @Override
    public void onCompletion() {
        super.onCompletion();
        this.task.profileEndNotification();
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

        int jobId = task.getJobId();
        NIOTaskResult tr = new NIOTaskResult(jobId, super.getResults());
        Connection c = TM.startConnection(n);
        CommandNIOTaskDone cmd = new CommandNIOTaskDone(tr, successful, task.getProfile(), task.getHistory().toString(),
            this.getException());
        c.sendCommand(cmd);
        c.finishConnection();
    }
}
