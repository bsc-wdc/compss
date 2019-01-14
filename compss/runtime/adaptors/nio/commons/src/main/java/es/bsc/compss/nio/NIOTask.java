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
package es.bsc.compss.nio;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation.TaskType;
import es.bsc.compss.types.job.Job.JobHistory;
import es.bsc.compss.types.resources.MethodResourceDescription;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


/**
 * Representation of a Task *
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

    /**
     * New NIO Task
     */
    public NIOTask() {
        // Only for externalization
    }

    /**
     * Creates a new task instance with the given parameters
     *
     * @param lang
     * @param workerDebug
     * @param impl
     * @param hasTarget
     * @param params
     * @param numReturns
     * @param numParams
     * @param reqs
     * @param slaveWorkersNodeNames
     * @param taskId
     * @param taskType
     * @param jobId
     * @param hist
     * @param transferGroupId
     */
    public NIOTask(Lang lang, boolean workerDebug, AbstractMethodImplementation impl, boolean hasTarget, int numReturns, LinkedList<NIOParam> params, int numParams, MethodResourceDescription reqs, List<String> slaveWorkersNodeNames,
            int taskId, TaskType taskType, int jobId, JobHistory hist, int transferGroupId) {
        this.lang = lang;
        this.workerDebug = workerDebug;
        this.impl = impl;
        this.arguments = new LinkedList<>();
        this.results = new LinkedList<>();

        Iterator<NIOParam> paramItr = params.descendingIterator();

        if (this.lang == Lang.PYTHON) {
            // Python params are in a different order
            if (hasTarget) {
                NIOParam p = paramItr.next();
                target = p;
            }
            for (int rIdx = 0; rIdx < numReturns; rIdx++) {
                NIOParam p = paramItr.next();
                results.addFirst(p);
            }
        } else {
            // C and Java params
            for (int rIdx = 0; rIdx < numReturns; rIdx++) {
                NIOParam p = paramItr.next();
                results.addFirst(p);
            }
            if (hasTarget) {
                NIOParam p = paramItr.next();
                target = p;
            }
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
     * Returns the task lang
     *
     * @return
     */
    @Override
    public Lang getLang() {
        return this.lang;
    }

    /**
     * Returns if the worker debug is enabled or not
     *
     * @return
     */
    @Override
    public boolean isDebugEnabled() {
        return this.workerDebug;
    }

    /**
     * Returns the method definition
     *
     * @return
     */
    public String getMethodDefinition() {
        return this.impl.getMethodDefinition();
    }

    /**
     * Returns the method implementation type
     *
     * @return
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
     * Returns the number of return parameters of the task
     *
     * @return
     */
    public int getNumReturns() {
        return this.numReturns;
    }

    /**
     * Returns the task id
     *
     * @return
     */
    @Override
    public int getTaskId() {
        return this.taskId;
    }

    /**
     * Returns the task type
     *
     * @return
     */
    @Override
    public TaskType getTaskType() {
        return this.taskType;
    }

    /**
     * Returns the job id
     *
     * @return
     */
    @Override
    public int getJobId() {
        return this.jobId;
    }

    /**
     * Returns the job history
     *
     * @return
     */
    @Override
    public JobHistory getHistory() {
        return this.history;
    }

    /**
     * Returns the transfer group id
     *
     * @return
     */
    public int getTransferGroupId() {
        return this.transferGroupId;
    }

    /**
     * Returns the resource description needed for the task execution
     *
     * @return
     */
    @Override
    public MethodResourceDescription getRequirements() {
        return this.reqs;
    }

    /**
     * Returns the slave workers node names
     *
     * @return
     */
    @Override
    public List<String> getSlaveNodesNames() {
        return this.slaveWorkersNodeNames;
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
        this.slaveWorkersNodeNames = (ArrayList<String>) in.readObject();
        this.taskType = TaskType.values()[in.readInt()];
        this.taskId = in.readInt();
        this.jobId = in.readInt();
        this.history = (JobHistory) in.readObject();

        this.transferGroupId = in.readInt();
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
