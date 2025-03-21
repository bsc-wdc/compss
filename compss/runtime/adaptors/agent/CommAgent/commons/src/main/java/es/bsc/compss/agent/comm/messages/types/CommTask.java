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

package es.bsc.compss.agent.comm.messages.types;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.job.JobHistory;
import es.bsc.compss.types.resources.MethodResourceDescription;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;


/**
 * This class is a container describing a task that is submitted to a CommAgent.
 */
public class CommTask extends NIOTask {

    private CommResource orchestrator;
    private String ceSignature;


    public CommTask() {
        // Only for externalisation
    }

    /**
     * Creates a new task instance with the given parameters.
     *
     * @param lang Task language.
     * @param workerDebug Worker debug level.
     * @param ceSignature Signature of the CE to execute.
     * @param impl Implementation to execute.
     * @param parallelismSource Interface class to parallelize the code
     * @param hasTarget Whether the task has a target object or not.
     * @param params List of task parameters.
     * @param numReturns Number of returns.
     * @param slaveWorkersNodeNames Slave node names.
     * @param taskId Task Id.
     * @param jobId Job Id.
     * @param hist Job history.
     * @param transferGroupId Transfer group Id.
     * @param onFailure Behavior in case of execution failure.
     * @param timeOut Task Deadline
     * @param orchestrator CommResource that will be notified at the end of the task
     */
    public CommTask(Lang lang, boolean workerDebug, String ceSignature, AbstractMethodImplementation impl,
        String parallelismSource, boolean hasTarget, int numReturns, LinkedList<NIOParam> params,
        List<String> slaveWorkersNodeNames, int taskId, int jobId, JobHistory hist, int transferGroupId,
        OnFailure onFailure, long timeOut, CommResource orchestrator) {
        super(lang, workerDebug, impl, parallelismSource, hasTarget, numReturns, params, slaveWorkersNodeNames, taskId,
            jobId, hist, transferGroupId, onFailure, timeOut, null, null);

        this.orchestrator = orchestrator;
        this.ceSignature = ceSignature;
    }

    /**
     * Creates a new task instance with the given parameters.
     *
     * @param lang Task language.
     * @param workerDebug Worker debug level.
     * @param ceSignature Signature of the CE to execute.
     * @param impl Implementation to execute.
     * @param parallelismSource Interface class to parallelize the code
     * @param arguments List of task's method arguments.
     * @param target Task's method callee
     * @param results List of task's method results.
     * @param slaveWorkersNodeNames Slave node names.
     * @param taskId Task Id.
     * @param jobId Job Id.
     * @param hist Job history.
     * @param transferGroupId Transfer group Id.
     * @param onFailure Behavior in case of execution failure.
     * @param timeOut Task deadline
     * @param orchestrator CommResource that will be notified at the end of the task
     */
    public CommTask(Lang lang, boolean workerDebug, String ceSignature, AbstractMethodImplementation impl,
        String parallelismSource, LinkedList<NIOParam> arguments, NIOParam target, LinkedList<NIOParam> results,
        List<String> slaveWorkersNodeNames, int taskId, int jobId, JobHistory hist, int transferGroupId,
        OnFailure onFailure, long timeOut, CommResource orchestrator) {

        super(lang, workerDebug, impl, parallelismSource, arguments, target, results, slaveWorkersNodeNames, taskId,
            jobId, hist, transferGroupId, onFailure, timeOut);

        this.orchestrator = orchestrator;
        this.ceSignature = ceSignature;
    }

    public CommResource getOrchestrator() {
        return this.orchestrator;
    }

    public String getCeSignature() {
        return ceSignature;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        this.orchestrator = (CommResource) in.readObject();
        this.ceSignature = in.readUTF();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(this.orchestrator);
        out.writeUTF(ceSignature);
    }

    @Override
    protected void dumpContent(StringBuilder sb) {
        super.dumpContent(sb);
        sb.append(",\"orchestrator\":").append(orchestrator);
        sb.append(",\"ce_signature\":");
        if (this.ceSignature == null) {
            sb.append("null");
        } else {
            sb.append("\"").append(this.ceSignature).append("\"");
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        this.dumpContent(sb);
        sb.append("}");
        return sb.toString();
    }

}
