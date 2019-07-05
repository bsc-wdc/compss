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
package es.bsc.compss.nio;

import es.bsc.compss.COMPSsConstants.Lang;
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

    private LinkedList<NIOParam> arguments;
    private NIOParam target;
    private LinkedList<NIOParam> results;
    private MethodResourceDescription reqs;
    private List<String> slaveWorkersNodeNames;
    private int taskId;
    private TaskType taskType;
    private int jobId;
    private JobHistory history;
    private int transferGroupId;
    private int numReturns;
    private int timeOut;

    /**
     * New NIO Task.
     */
    public NIOTask() {
        // Only for externalization
    }

    /**
     * Creates a new task instance with the given parameters.
     *
     * @param lang                  Task language.
     * @param workerDebug           Worker debug level.
     * @param impl                  Implementation to execute.
     * @param hasTarget             Whether the task has a target object or not.
     * @param params                List of task parameters.
     * @param numReturns            Number of returns.
     * @param numParams             Number of parameters.
     * @param reqs                  Requirements.
     * @param slaveWorkersNodeNames Slave node names.
<<<<<<< HEAD
     * @param taskId                Task Id.
     * @param taskType              Task type.
     * @param jobId                 Job Id.
     * @param hist                  Job history.
     * @param transferGroupId       Transfer group Id.
=======
     * @param taskId Task Id.
     * @param taskType Task type.
     * @param jobId Job Id.
     * @param hist Job history.
     * @param transferGroupId Transfer group Id.
     * @param timeOut
>>>>>>> Java runtime addition of task time out and task groups
     */
    public NIOTask(Lang lang, boolean workerDebug, AbstractMethodImplementation impl, boolean hasTarget, int numReturns,
            LinkedList<NIOParam> params, int numParams, MethodResourceDescription reqs,
            List<String> slaveWorkersNodeNames, int taskId, TaskType taskType, int jobId, JobHistory hist,
            int transferGroupId, int timeOut) {

        this.lang = lang;
        this.workerDebug = workerDebug;
        this.impl = impl;
        this.arguments = new LinkedList<>();
        this.results = new LinkedList<>();
        this.timeOut = timeOut;

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

        this.reqs = reqs;
        this.slaveWorkersNodeNames = slaveWorkersNodeNames;
        this.taskType = taskType;
        this.taskId = taskId;
        this.jobId = jobId;
        this.history = hist;
        this.transferGroupId = transferGroupId;
        this.numReturns = numReturns;
    }

    /**
     * Creates a new task instance with the given parameters.
     *
     * @param lang                  Task language.
     * @param workerDebug           Worker debug level.
     * @param impl                  Implementation to execute.
     * @param arguments             List of task's method arguments.
     * @param target                Task's method callee
     * @param results               List of task's method results.
     * @param slaveWorkersNodeNames Slave node names.
     * @param taskId                Task Id.
     * @param jobId                 Job Id.
     * @param hist                  Job history.
     * @param transferGroupId       Transfer group Id.
     */
    public NIOTask(Lang lang, boolean workerDebug, AbstractMethodImplementation impl,
            LinkedList<NIOParam> arguments, NIOParam target, LinkedList<NIOParam> results,
            List<String> slaveWorkersNodeNames,
            int taskId, int jobId, JobHistory hist, int transferGroupId) {

        this.lang = lang;
        this.workerDebug = workerDebug;
        this.impl = impl;
        this.arguments = new LinkedList<>();
        this.results = new LinkedList<>();

        this.arguments = arguments;
        this.target = target;
        this.results = results;

        this.reqs = impl.getRequirements();
        this.slaveWorkersNodeNames = slaveWorkersNodeNames;
        this.taskType = impl.getTaskType();
        this.taskId = taskId;
        this.jobId = jobId;
        this.history = hist;
        this.transferGroupId = transferGroupId;
        this.numReturns = results.size();
    }

    /**
     * Returns the task language.
     *
     * @return The task language.
     */
    @Override
    public Lang getLang() {
        return this.lang;
    }

    /**
     * Returns whether the worker debug is enabled or not.
     *
     * @return {@literal true} if the worker debug is enabled, {@literal false} otherwise.
     */
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

    /**
     * Returns the method implementation type.
     *
     * @return The method implementation type.
     */
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

    /**
     * Returns the task Id.
     *
     * @return The task Id.
     */
    @Override
    public int getTaskId() {
        return this.taskId;
    }

    /**
     * Returns the task type.
     *
     * @return The task type.
     */
    @Override
    public TaskType getTaskType() {
        return this.taskType;
    }

    /**
     * Returns the job Id.
     *
     * @return The job Id.
     */
    @Override
    public int getJobId() {
        return this.jobId;
    }

    /**
     * Returns the job history.
     *
     * @return The job history.
     */
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

    /**
     * Returns the resource description needed for the task execution.
     *
     * @return The resource description needed for the task execution.
     */
    @Override
    public MethodResourceDescription getRequirements() {
        return this.reqs;
    }

    /**
     * Returns the slave workers node names.
     *
     * @return The slave workers node names.
     */
    @Override
    public List<String> getSlaveNodesNames() {
        return this.slaveWorkersNodeNames;
    }
    
    @Override
    public int getTimeOut() {
        return this.timeOut;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.lang = Lang.valueOf((String) in.readObject());
        this.workerDebug = in.readBoolean();
        this.impl = (AbstractMethodImplementation) in.readObject();
        this.numReturns = in.readInt();
        this.arguments = (LinkedList<NIOParam>) in.readObject();
        this.target = (NIOParam) in.readObject();
        this.results = (LinkedList<NIOParam>) in.readObject();
        this.reqs = (MethodResourceDescription) in.readObject();
        this.slaveWorkersNodeNames = (List<String>) in.readObject();
        this.taskType = TaskType.values()[in.readInt()];
        this.taskId = in.readInt();
        this.jobId = in.readInt();
        this.history = (JobHistory) in.readObject();

        this.transferGroupId = in.readInt();
        this.timeOut = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.lang.toString());
        out.writeBoolean(this.workerDebug);
        out.writeObject(this.impl);
        out.writeInt(this.numReturns);
        out.writeObject(this.arguments);
        out.writeObject(this.target);
        out.writeObject(this.results);
        out.writeObject(this.reqs);
        out.writeObject(this.slaveWorkersNodeNames);
        out.writeInt(this.taskType.ordinal());
        out.writeInt(this.taskId);
        out.writeInt(this.jobId);
        out.writeObject(this.history);
        out.writeInt(this.transferGroupId);
        out.writeInt(this.timeOut);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[TASK ");
        sb.append("[LANG= ").append(this.lang).append("]");
        sb.append("[TASK TYPE= ").append(this.taskType).append("]");
        sb.append("[TASK ID= ").append(this.taskId).append("]");
        sb.append("[JOB ID= ").append(this.jobId).append("]");
        sb.append("[HISTORY= ").append(this.history).append("]");
        sb.append("[IMPLEMENTATION= ").append(this.impl.getMethodDefinition()).append("]");
        sb.append(" [PARAMS ");
        for (NIOParam param : this.arguments) {
            sb.append(param);
        }
        if (target != null) {
            sb.append(target);
        }
        for (NIOParam param : this.results) {
            sb.append(param);
        }
        sb.append("]");

        sb.append("[REQUIREMENTS= ").append(this.reqs).append("]");

        sb.append("[SLAVE_WORKERS_NODE_NAMES= ");
        for (String name : this.slaveWorkersNodeNames) {
            sb.append("[SW_NAME=").append(name).append("]");
        }
        sb.append("]");

        sb.append("]");
        return sb.toString();
    }

}
