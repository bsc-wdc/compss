package integratedtoolkit.nio;

import integratedtoolkit.types.implementations.AbstractMethodImplementation;
import integratedtoolkit.types.implementations.AbstractMethodImplementation.MethodType;
import integratedtoolkit.types.job.Job.JobHistory;
import integratedtoolkit.types.resources.MethodResourceDescription;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


/**
 * Representation of a Task *
 */
public class NIOTask implements Externalizable {

    private String lang;
    private boolean workerDebug;
    private AbstractMethodImplementation impl;
    private boolean hasTarget;
    private boolean hasReturn;
    private LinkedList<NIOParam> params;
    private MethodResourceDescription reqs;
    private List<String> slaveWorkersNodeNames;
    private int taskId;
    private int taskType;
    private int jobId;
    private JobHistory hist;
    private int transferGroupId;
    private int numParams;


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
     * @param numParams
     * @param reqs
     * @param slaveWorkersNodeNames
     * @param taskId
     * @param taskType
     * @param jobId
     * @param hist
     * @param transferGroupId
     */
    public NIOTask(String lang, boolean workerDebug, AbstractMethodImplementation impl, boolean hasTarget, boolean hasReturn,
            LinkedList<NIOParam> params, int numParams, MethodResourceDescription reqs, List<String> slaveWorkersNodeNames, 
            int taskId, int taskType, int jobId, JobHistory hist, int transferGroupId) {

        this.lang = lang;
        this.workerDebug = workerDebug;
        this.impl = impl;
        this.hasTarget = hasTarget;
        this.hasReturn = hasReturn;
        this.params = params;
        this.reqs = reqs;
        this.slaveWorkersNodeNames = slaveWorkersNodeNames;
        this.taskType = taskType;
        this.taskId = taskId;
        this.jobId = jobId;
        this.hist = hist;
        this.numParams = numParams;
        this.transferGroupId = transferGroupId;
    }

    /**
     * Returns the task lang
     * 
     * @return
     */
    public String getLang() {
        return this.lang;
    }

    /**
     * Returns if the worker debug is enabled or not
     * 
     * @return
     */
    public boolean isWorkerDebug() {
        return this.workerDebug;
    }

    /**
     * Returns the method type
     * 
     * @return
     */
    public MethodType getMethodType() {
        return this.impl.getMethodType();
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
    public AbstractMethodImplementation getMethodImplementation() {
        return this.impl;
    }

    /**
     * Returns if the task has target or not
     * 
     * @return
     */
    public boolean isHasTarget() {
        return this.hasTarget;
    }
    
    /**
     * Returns if the task has return value or not
     * 
     * @return
     */
    public boolean isHasReturn() {
        return this.hasReturn;
    }

    /**
     * Returns the number of parameters of the task
     * 
     * @return
     */
    public int getNumParams() {
        return this.numParams;
    }

    /**
     * Returns the task parameters
     * 
     * @return
     */
    public LinkedList<NIOParam> getParams() {
        return this.params;
    }

    /**
     * Returns the task id
     * 
     * @return
     */
    public int getTaskId() {
        return this.taskId;
    }

    /**
     * Returns the task type
     * 
     * @return
     */
    public int getTaskType() {
        return this.taskType;
    }

    /**
     * Returns the job id
     * 
     * @return
     */
    public int getJobId() {
        return this.jobId;
    }

    /**
     * Returns the job history
     * 
     * @return
     */
    public JobHistory getHist() {
        return this.hist;
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
    public MethodResourceDescription getResourceDescription() {
        return this.reqs;
    }

    /**
     * Returns the slave workers node names
     * 
     * @return
     */
    public List<String> getSlaveWorkersNodeNames() {
        return this.slaveWorkersNodeNames;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.lang = (String) in.readObject();
        this.workerDebug = in.readBoolean();
        this.impl = (AbstractMethodImplementation) in.readObject();
        this.hasTarget = in.readBoolean();
        this.hasReturn = in.readBoolean();
        this.params = (LinkedList<NIOParam>) in.readObject();
        this.reqs = (MethodResourceDescription) in.readObject();
        this.slaveWorkersNodeNames = (ArrayList<String>) in.readObject();
        this.taskType = in.readInt();
        this.taskId = in.readInt();
        this.jobId = in.readInt();
        this.hist = (JobHistory) in.readObject();
        this.numParams = in.readInt();
        this.transferGroupId = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.lang);
        out.writeBoolean(this.workerDebug);
        out.writeObject(this.impl);
        out.writeBoolean(this.hasTarget);
        out.writeBoolean(this.hasReturn);
        out.writeObject(this.params);
        out.writeObject(this.reqs);
        out.writeObject(this.slaveWorkersNodeNames);
        out.writeInt(this.taskType);
        out.writeInt(this.taskId);
        out.writeInt(this.jobId);
        out.writeObject(this.hist);
        out.writeInt(this.numParams);
        out.writeInt(this.transferGroupId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[TASK ");
        sb.append("[LANG= ").append(this.lang).append("]");
        sb.append("[TASK TYPE= ").append(this.taskType).append("]");
        sb.append("[TASK ID= ").append(this.taskId).append("]");
        sb.append("[JOB ID= ").append(this.jobId).append("]");
        sb.append("[HISTORY= ").append(this.hist).append("]");
        sb.append("[IMPLEMENTATION= ").append(this.impl.getMethodDefinition()).append("]");
        sb.append(" [PARAMS ");
        for (NIOParam param : this.params) {
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
