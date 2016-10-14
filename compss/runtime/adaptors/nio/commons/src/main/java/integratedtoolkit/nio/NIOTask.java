package integratedtoolkit.nio;

import integratedtoolkit.types.implementations.AbstractMethodImplementation;
import integratedtoolkit.types.implementations.AbstractMethodImplementation.MethodType;
import integratedtoolkit.types.job.Job.JobHistory;
import integratedtoolkit.types.resources.MethodResourceDescription;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;


public class NIOTask implements Externalizable {

    private String lang;
    private boolean workerDebug;
    private AbstractMethodImplementation impl;
    private boolean hasTarget;
    private LinkedList<NIOParam> params;
    private MethodResourceDescription reqs;
    private int taskId;
    private int taskType;
    private int jobId;
    private JobHistory hist;
    private int transferGroupId;
    private int numParams;
    

    public NIOTask() {

    }

    public NIOTask(String lang, boolean workerDebug, AbstractMethodImplementation impl, boolean hasTarget, 
            LinkedList<NIOParam> params, int numParams, MethodResourceDescription reqs, int taskId, int taskType, int jobId, 
            JobHistory hist, int transferGroupId) {

        this.lang = lang;
        this.workerDebug = workerDebug;
        this.impl = impl;
        this.hasTarget = hasTarget;
        this.params = params;
        this.reqs = reqs;
        this.taskType = taskType;
        this.taskId = taskId;
        this.jobId = jobId;
        this.hist = hist;
        this.numParams = numParams;
        this.transferGroupId = transferGroupId;
    }

    public String getLang() {
        return lang;
    }

    public boolean isWorkerDebug() {
        return workerDebug;
    }
    
    public MethodType getMethodType() {
        return this.impl.getMethodType();
    }

    public String getMethodDefinition() {
        return this.impl.getMethodDefinition();
    }
    
    public AbstractMethodImplementation getMethodImplementation() {
        return this.impl;
    }

    public boolean isHasTarget() {
        return hasTarget;
    }

    public LinkedList<NIOParam> getParams() {
        return params;
    }

    public int getTaskId() {
        return taskId;
    }

    public int getTaskType() {
        return taskType;
    }

    public int getJobId() {
        return jobId;
    }

    public JobHistory getHist() {
        return hist;
    }

    public int getTransferGroupId() {
        return transferGroupId;
    }

    public MethodResourceDescription getResourceDescription() {
        return reqs;
    }

    public int getNumParams() {
        return numParams;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        lang = (String) in.readObject();
        workerDebug = in.readBoolean();
        impl = (AbstractMethodImplementation) in.readObject();
        hasTarget = in.readBoolean();
        params = (LinkedList<NIOParam>) in.readObject();
        reqs = (MethodResourceDescription) in.readObject();
        taskType = in.readInt();
        taskId = in.readInt();
        jobId = in.readInt();
        hist = (JobHistory) in.readObject();
        numParams = in.readInt();
        transferGroupId = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(lang);
        out.writeBoolean(workerDebug);
        out.writeObject(impl);
        out.writeBoolean(hasTarget);
        out.writeObject(params);
        out.writeObject(reqs);
        out.writeInt(taskType);
        out.writeInt(taskId);
        out.writeInt(jobId);
        out.writeObject(hist);
        out.writeInt(numParams);
        out.writeInt(transferGroupId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[TASK ");
        sb.append("[LANG= ").append(lang).append("]");
        sb.append("[TASK TYPE= ").append(taskType).append("]");
        sb.append("[TASK ID= ").append(taskId).append("]");
        sb.append("[JOB ID= ").append(jobId).append("]");
        sb.append("[HISTORY= ").append(hist).append("]");
        sb.append("[IMPLEMENTATION= ").append(impl.getMethodDefinition()).append("]");
        sb.append(" [PARAMS ");
        for (NIOParam param : params) {
            sb.append(param);
        }
        sb.append("]");
        sb.append("[REQUIREMENTS= ").append(reqs).append("]");
        sb.append("]");
        return sb.toString();
    }

}
