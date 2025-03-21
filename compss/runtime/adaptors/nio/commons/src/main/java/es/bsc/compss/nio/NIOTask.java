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
package es.bsc.compss.nio;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.job.JobHistory;
import es.bsc.compss.types.resources.MethodResourceDescription;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


/**
 * Representation of a Task.
 */
public class NIOTask implements Externalizable, Invocation {

    private Lang lang;
    private boolean workerDebug;
    private AbstractMethodImplementation impl;
    private String parallelismSource;

    private LinkedList<NIOParam> arguments;
    private NIOParam target;
    private LinkedList<NIOParam> results;
    private List<String> slaveWorkersNodeNames;
    private int taskId;
    private int jobId;
    private JobHistory history;
    private int transferGroupId;
    private int numReturns;
    private OnFailure onFailure;
    private long timeOut;

    private List<Integer> predecessors;
    private Integer numSuccessors;

    // Invocation Profile
    private final NIOTaskProfile profile;


    /**
     * New NIO Task.
     */
    public NIOTask() {
        // Only for externalization
        this.profile = new NIOTaskProfile();
    }

    /**
     * Creates a new task instance with the given parameters.
     *
     * @param lang Task language.
     * @param workerDebug Worker debug level.
     * @param impl Implementation to execute.
     * @param parallelismSource Identifier of the object describing how the task should be parallelized
     * @param hasTarget Whether the task has a target object or not.
     * @param params List of task parameters.
     * @param numReturns Number of returns.
     * @param slaveWorkersNodeNames Slave node names.
     * @param taskId Task Id.
     * @param jobId Job Id.
     * @param hist Job history.
     * @param transferGroupId Transfer group Id.
     * @param timeOut Task timeout.
     */
    public NIOTask(Lang lang, boolean workerDebug, AbstractMethodImplementation impl, String parallelismSource,
        boolean hasTarget, int numReturns, LinkedList<NIOParam> params, List<String> slaveWorkersNodeNames, int taskId,
        int jobId, JobHistory hist, int transferGroupId, OnFailure onFailure, long timeOut, List<Integer> predecessors,
        Integer numSuccessors) {

        this.lang = lang;
        this.workerDebug = workerDebug;
        this.impl = impl;
        this.parallelismSource = parallelismSource;
        this.arguments = new LinkedList<>();
        this.results = new LinkedList<>();
        this.onFailure = onFailure;
        this.timeOut = timeOut;
        this.predecessors = predecessors;
        this.numSuccessors = numSuccessors;

        Iterator<NIOParam> paramItr = params.descendingIterator();

        // C, Java and Python params (arguments + self + results)
        for (int rIdx = 0; rIdx < numReturns; rIdx++) {
            NIOParam p = paramItr.next();
            this.results.addFirst(p);
        }
        if (hasTarget) {
            NIOParam p = paramItr.next();
            this.target = p;
        }
        while (paramItr.hasNext()) {
            NIOParam p = paramItr.next();
            this.arguments.addFirst(p);
        }

        this.slaveWorkersNodeNames = slaveWorkersNodeNames;
        this.taskId = taskId;
        this.jobId = jobId;
        this.history = hist;
        this.transferGroupId = transferGroupId;
        this.numReturns = numReturns;
        this.profile = new NIOTaskProfile();
    }

    /**
     * Creates a new task instance with the given parameters.
     *
     * @param lang Task language.
     * @param workerDebug Worker debug level.
     * @param impl Implementation to execute.
     * @param parallelismSource Identifier of the object describing how the task should be parallelized
     * @param arguments List of task's method arguments.
     * @param target Task's method callee
     * @param results List of task's method results.
     * @param slaveWorkersNodeNames Slave node names.
     * @param taskId Task Id.
     * @param jobId Job Id.
     * @param hist Job history.
     * @param transferGroupId Transfer group Id.
     * @param timeOut Task deadline
     */
    public NIOTask(Lang lang, boolean workerDebug, AbstractMethodImplementation impl, String parallelismSource,
        LinkedList<NIOParam> arguments, NIOParam target, LinkedList<NIOParam> results,
        List<String> slaveWorkersNodeNames, int taskId, int jobId, JobHistory hist, int transferGroupId,
        OnFailure onFailure, long timeOut) {

        this.lang = lang;
        this.workerDebug = workerDebug;
        this.impl = impl;
        this.parallelismSource = parallelismSource;

        this.arguments = arguments;
        this.target = target;
        this.results = results;
        this.onFailure = onFailure;
        this.timeOut = timeOut;

        this.slaveWorkersNodeNames = slaveWorkersNodeNames;
        this.taskId = taskId;
        this.jobId = jobId;
        this.history = hist;
        this.transferGroupId = transferGroupId;
        this.numReturns = results.size();
        this.profile = new NIOTaskProfile();
    }

    @Override
    public Lang getLang() {
        return this.lang;
    }

    @Override
    public boolean isDebugEnabled() {
        return this.workerDebug;
    }

    /**
     * Returns the method definition.
     *
     * @return The method definition.
     */
    public String getMethodDefinition() {
        return this.impl.getMethodDefinition();
    }

    @Override
    public AbstractMethodImplementation getMethodImplementation() {
        return this.impl;
    }

    @Override
    public List<NIOParam> getParams() {
        return this.arguments;
    }

    @Override
    public NIOParam getTarget() {
        return this.target;
    }

    @Override
    public List<NIOParam> getResults() {
        return this.results;
    }

    /**
     * Returns the number of return parameters of the task.
     *
     * @return The number of return parameters of the task.
     */
    public int getNumReturns() {
        return this.numReturns;
    }

    @Override
    public int getTaskId() {
        return this.taskId;
    }

    @Override
    public TaskType getTaskType() {
        return this.impl.getTaskType();
    }

    @Override
    public int getJobId() {
        return this.jobId;
    }

    @Override
    public JobHistory getHistory() {
        return this.history;
    }

    /**
     * Returns the transfer group Id.
     *
     * @return The transfer group Id.
     */
    public int getTransferGroupId() {
        return this.transferGroupId;
    }

    @Override
    public MethodResourceDescription getRequirements() {
        return this.impl.getRequirements();
    }

    @Override
    public List<String> getSlaveNodesNames() {
        return this.slaveWorkersNodeNames;
    }

    @Override
    public OnFailure getOnFailure() {
        return this.onFailure;
    }

    @Override
    public boolean producesEmptyResultsOnFailure() {
        return this.onFailure != OnFailure.RETRY && this.onFailure != OnFailure.FAIL;
    }

    @Override
    public long getTimeOut() {
        return this.timeOut;
    }

    @Override
    public String getParallelismSource() {
        return this.parallelismSource;
    }

    @Override
    public List<Integer> getPredecessors() {
        return this.predecessors;
    }

    @Override
    public Integer getNumSuccessors() {
        return this.numSuccessors;
    }

    public void profileArrival() {
        profile.arrived();
    }

    public void profileFetchedData() {
        profile.dataFetched();
    }

    @Override
    public void executionStarts() {
        profile.executionStarts();
    }

    @Override
    public void executionEnds() {
        profile.executionEnds();
    }

    public void profileEndNotification() {
        profile.end();
    }

    public NIOTaskProfile getProfile() {
        return profile;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.lang = Lang.valueOf((String) in.readObject());
        this.workerDebug = in.readBoolean();
        this.impl = (AbstractMethodImplementation) in.readObject();
        boolean ceiDefined = in.readBoolean();
        if (ceiDefined) {
            this.parallelismSource = in.readUTF();
        }
        this.numReturns = in.readInt();
        this.arguments = (LinkedList<NIOParam>) in.readObject();
        this.target = (NIOParam) in.readObject();
        this.results = (LinkedList<NIOParam>) in.readObject();
        this.slaveWorkersNodeNames = (List<String>) in.readObject();
        this.taskId = in.readInt();
        this.jobId = in.readInt();
        this.history = (JobHistory) in.readObject();
        this.transferGroupId = in.readInt();
        this.onFailure = (OnFailure) in.readObject();
        this.timeOut = in.readLong();
        this.predecessors = (List<Integer>) in.readObject();
        this.numSuccessors = (Integer) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.lang.toString());
        out.writeBoolean(this.workerDebug);
        out.writeObject(this.impl);
        boolean ceiDefined = this.parallelismSource != null && !this.parallelismSource.isEmpty();
        out.writeBoolean(ceiDefined);
        if (ceiDefined) {
            out.writeUTF(this.parallelismSource);
        }
        out.writeInt(this.numReturns);
        out.writeObject(this.arguments);
        out.writeObject(this.target);
        out.writeObject(this.results);
        out.writeObject(this.slaveWorkersNodeNames);
        out.writeInt(this.taskId);
        out.writeInt(this.jobId);
        out.writeObject(this.history);
        out.writeInt(this.transferGroupId);
        out.writeObject(this.onFailure);
        out.writeLong(this.timeOut);
        out.writeObject(this.predecessors);
        out.writeObject(this.numSuccessors);
    }

    protected void dumpContent(StringBuilder sb) {
        sb.append("\"lang\":\"").append(this.lang).append("\",");
        sb.append("\"task_id\":").append(this.taskId).append(",");
        sb.append("\"job_id\":").append(this.jobId).append(",");
        sb.append("\"history\":\"").append(this.history).append("\",");
        sb.append("\"implementation\":").append(this.impl.toJSON()).append(",");
        sb.append("\"parallelism_source\":")
            .append(this.parallelismSource == null ? "null" : "\"" + this.parallelismSource + "\"").append(",");
        sb.append("\"params\":[");
        Iterator<NIOParam> argsItr = this.arguments.iterator();
        if (argsItr.hasNext()) {
            sb.append(argsItr.next());
        }
        while (argsItr.hasNext()) {
            sb.append(",").append(argsItr.next());
        }
        sb.append(" ],\"target\":");
        if (target != null) {
            sb.append(target);
        } else {
            sb.append("null");
        }
        sb.append(",\"results\":[");
        Iterator<NIOParam> resItr = this.arguments.iterator();
        if (resItr.hasNext()) {
            sb.append(resItr.next());
        }
        while (resItr.hasNext()) {
            sb.append(",").append(resItr.next());
        }
        sb.append("],");

        sb.append("\"slave_workers_node_names\":[");
        for (String name : this.slaveWorkersNodeNames) {
            sb.append("{\"sw_name\":").append(name).append("}");
        }
        sb.append("]");

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        this.dumpContent(sb);
        sb.append("}");
        return sb.toString();
    }

}
