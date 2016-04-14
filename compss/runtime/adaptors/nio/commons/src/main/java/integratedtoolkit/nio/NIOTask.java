package integratedtoolkit.nio;

import integratedtoolkit.types.job.Job.JobHistory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;

public class NIOTask implements Externalizable {

    public String lang;
    public String installDir;
    public String libPath;
    public String appDir;
    public String classPath;
    public boolean workerDebug;

    private String className;
    private String methodName;
    private boolean hasTarget;
    private LinkedList<NIOParam> params;
    private int taskId;
    private int taskType;
    private int jobId;
    private JobHistory hist;
    private int transferGroupId;
    private int numParams;

    public NIOTask() {

    }

    public NIOTask(String lang, String installDir, String libPath, String appDir, String classPath, boolean workerDebug, String className, String methodName, boolean hasTarget, LinkedList<NIOParam> params, int numParams, int taskId, int taskType, int jobId, JobHistory hist, int transferGroupId) {

        this.lang = lang;
        this.installDir = installDir;
        this.libPath = libPath;
        this.appDir = appDir;
        this.classPath = classPath;
        this.workerDebug = workerDebug;

        this.className = className;
        this.methodName = methodName;
        this.hasTarget = hasTarget;
        this.params = params;
        this.taskType = taskType;
        this.taskId = taskId;
        this.jobId = jobId;
        this.hist = hist;
        this.numParams = numParams;
        this.transferGroupId = transferGroupId;
    }

    public String getClassName() {
        return className;
    }

    public String getMethodName() {
        return methodName;
    }

    public boolean isHasTarget() {
        return hasTarget;
    }

    public LinkedList<NIOParam> getParams() {
        return params;
    }

    public int getJobId() {
        return jobId;
    }

    public int getTaskId() {
        return taskId;
    }

    public int getTaskType() {
        return taskType;
    }

    public JobHistory getHist() {
        return hist;
    }

    public int getNumParams() {
        return numParams;
    }

    public int getTransferGroupId() {
        return transferGroupId;
    }

    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        lang = (String) in.readObject();
        installDir = (String) in.readObject();
        libPath = (String) in.readObject();
        appDir = (String) in.readObject();
        classPath = (String) in.readObject();
        workerDebug = in.readBoolean();
        className = (String) in.readObject();
        methodName = (String) in.readObject();
        hasTarget = in.readBoolean();
        params = (LinkedList<NIOParam>) in.readObject();
        taskType = in.readInt();
        taskId = in.readInt();
        jobId = in.readInt();
        hist = (JobHistory) in.readObject();
        numParams = in.readInt();
        transferGroupId = in.readInt();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(lang);
        out.writeObject(installDir);
        out.writeObject(libPath);
        out.writeObject(appDir);
        out.writeObject(classPath);
        out.writeBoolean(workerDebug);
        out.writeObject(className);
        out.writeObject(methodName);
        out.writeBoolean(hasTarget);
        out.writeObject(params);
        out.writeInt(taskType);
        out.writeInt(taskId);
        out.writeInt(jobId);
        out.writeObject(hist);
        out.writeInt(numParams);
        out.writeInt(transferGroupId);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("[TASK ");
        sb.append("[LANG= ").append(lang).append("]");
        sb.append("[TASK TYPE= ").append(taskType).append("]");
        sb.append("[TASK ID= ").append(taskId).append("]");
        sb.append("[JOB ID= ").append(jobId).append("]");
        sb.append("[HISTORY= ").append(hist).append("]");
        sb.append("[CLASS= ").append(className).append("]");
        sb.append("[METHOD= ").append(methodName).append("]");
        sb.append(" [PARAMS ");
        for (NIOParam param : params) {
            sb.append(param);
        }
        sb.append("]");
        sb.append("]");
        return sb.toString();
    }
}
