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

import es.bsc.comm.Connection;
import es.bsc.comm.TransferManager;
import es.bsc.comm.nio.NIONode;
import es.bsc.compss.agent.AppMonitor;
import es.bsc.compss.agent.comm.messages.types.CommResource;
import es.bsc.compss.agent.comm.messages.types.CommResult;
import es.bsc.compss.agent.comm.messages.types.CommTask;
import es.bsc.compss.agent.types.ApplicationParameter;
import es.bsc.compss.agent.types.RemoteDataLocation;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOResult;
import es.bsc.compss.nio.NIOResultCollection;
import es.bsc.compss.nio.NIOTaskResult;
import es.bsc.compss.nio.commands.CommandDataReceived;
import es.bsc.compss.nio.commands.CommandNIOTaskDone;
import es.bsc.compss.worker.COMPSsException;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Monitor to detect changes on a task state and notify the orchestrator that commanded its execution.
 */
public class CommAppMonitor extends AppMonitor {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.AGENT);

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
    public CommAppMonitor(ApplicationParameter[] args, ApplicationParameter target, ApplicationParameter[] results,
        CommResource orchestrator, CommTask request) {
        super(args, target, results);
        this.orchestrator = orchestrator;
        this.successful = false;
        this.task = request;
        this.task.profileArrival();
    }

    private NIOResult createCommResult(TaskResult taskRes) {
        if (taskRes == null) {
            return new CommResult();
        }
        NIOResult res;
        if (taskRes.isCollective()) {
            List<NIOResult> elements = new LinkedList<>();
            CollectionTaskResult cResult = (CollectionTaskResult) taskRes;
            for (TaskResult subResult : cResult.getSubelements()) {
                elements.add(createCommResult(subResult));
            }
            res = new NIOResultCollection(elements);
        } else {
            CommResult commRes = new CommResult();
            Collection<RemoteDataLocation> rdl = taskRes.getLocations();
            if (rdl != null) {
                commRes.setRemoteData(rdl);
            }
            res = commRes;
        }
        return res;
    }

    @Override
    public CommTaskMonitor getTaskMonitor() {
        return new CommTaskMonitor();
    }

    @Override
    public void stalledApplication() {
        super.stalledApplication();
    }

    @Override
    protected void specificOnCancellation() {

    }

    @Override
    protected void specificOnException(COMPSsException e) {

    }

    @Override
    protected void specificOnCompletion() {
        if (CommAppMonitor.this.orchestrator != null) {
            notifyEnd();
        }

    }

    @Override
    protected void specificOnFailure() {
        if (CommAppMonitor.this.orchestrator != null) {
            notifyEnd();
        }
    }

    /**
     * Notifies the end of the task.
     */
    private void notifyEnd() {
        int jobId = task.getJobId();
        NIOTaskResult tr = new NIOTaskResult(jobId);

        for (TaskResult param : this.getResults()) {
            tr.addParamResult(createCommResult(param));
        }

        NIONode n = new NIONode(orchestrator.getName(), orchestrator.getPort());
        Connection c = TM.startConnection(n);
        CommandNIOTaskDone cmd = new CommandNIOTaskDone(tr, successful, task.getProfile(), task.getHistory().toString(),
            this.getException());
        c.sendCommand(cmd);
        c.finishConnection();
    }


    protected class CommTaskMonitor extends UniqueTaskMonitor {

        @Override
        protected void specificOnCreation() {
        }

        @Override
        protected void specificOnAccessesProcessed() {
        }

        @Override
        protected void specificOnSchedule() {
        }

        @Override
        protected void specificOnSubmission() {
        }

        @Override
        protected void specificOnDataReception() {
            CommAppMonitor.this.task.profileFetchedData();
            if (CommAppMonitor.this.orchestrator != null) {
                NIONode n = new NIONode(orchestrator.getName(), orchestrator.getPort());

                int transferGroupId = CommAppMonitor.this.task.getTransferGroupId();

                Connection c = TM.startConnection(n);
                CommandDataReceived cmd = new CommandDataReceived(transferGroupId);
                c.sendCommand(cmd);
                c.finishConnection();
            }
        }

        @Override
        protected void specificOnExecutionStart() {
            task.getProfile().executionStarts();
        }

        @Override
        protected void specificOnExecutionStartAt(long ts) {
            task.getProfile().executionStartsAt(ts);
        }

        @Override
        protected void specificOnExecutionEnd() {
            task.getProfile().executionEnds();
        }

        @Override
        protected void specificOnExecutionEndAt(long ts) {
            task.getProfile().executionEndsAt(ts);
        }

        @Override
        protected void specificOnAbortedExecution() {
        }

        @Override
        protected void specificOnErrorExecution() {
        }

        @Override
        protected void specificOnFailedExecution() {
        }

        @Override
        protected void specificOnSuccessfulExecution() {
            CommAppMonitor.this.successful = true;
        }

        @Override
        protected void specificOnCancellation() {
        }

        @Override
        protected void specificOnException(COMPSsException e) {
        }

        @Override
        public void specificOnCompletion() {
            CommAppMonitor.this.task.profileEndNotification();
        }

        @Override
        public void specificOnFailure() {

        }
    }
}
