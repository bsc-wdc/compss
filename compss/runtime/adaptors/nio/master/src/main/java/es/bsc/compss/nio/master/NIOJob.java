package es.bsc.compss.nio.master;

import java.util.LinkedList;
import java.util.List;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.commands.Data;

import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.RAccessId;
import es.bsc.compss.types.data.DataAccessId.RWAccessId;
import es.bsc.compss.types.data.DataAccessId.WAccessId;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.implementations.Implementation.TaskType;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.job.JobListener.JobEndStatus;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.annotations.parameter.DataType;


public class NIOJob extends Job<NIOWorkerNode> {

    private final List<String> slaveWorkersNodeNames;


    public NIOJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res, List<String> slaveWorkersNodeNames,
            JobListener listener) {

        super(taskId, taskParams, impl, res, listener);
        this.slaveWorkersNodeNames = slaveWorkersNodeNames;
    }

    @Override
    public TaskType getType() {
        return TaskType.METHOD;
    }

    @Override
    public String getHostName() {
        return worker.getName();
    }

    @Override
    public void submit() throws Exception {
        // Prepare the job
        logger.info("Submit NIOJob with ID " + jobId);
        NIOAdaptor.submitTask(this);
    }

    public NIOTask prepareJob() {
        AbstractMethodImplementation absMethodImpl = (AbstractMethodImplementation) this.impl;

        // If it is a native method, check that methodname is defined (otherwise define it from job parameters)
        // This is a workarround for Python
        if (absMethodImpl.getMethodType().equals(MethodType.METHOD)) {
            MethodImplementation mImpl = (MethodImplementation) absMethodImpl;
            String methodName = mImpl.getAlternativeMethodName();
            if (methodName == null || methodName.isEmpty()) {
                mImpl.setAlternativeMethodName(taskParams.getName());
            }
        }

        boolean hasTarget = taskParams.hasTargetObject();
        boolean hasReturn = taskParams.hasReturnValue();
        LinkedList<NIOParam> params = addParams();
        MethodResourceDescription reqs = absMethodImpl.getRequirements();
        int numParams = params.size();
        if (taskParams.hasReturnValue()) {
            numParams--;
        }

        // Create NIOTask
        NIOTask nt = new NIOTask(LANG, debug, absMethodImpl, hasTarget, hasReturn, params, numParams, reqs, this.slaveWorkersNodeNames,
                this.taskId, this.taskParams.getId(), this.jobId, this.history, this.transferId);

        return nt;
    }

    private LinkedList<NIOParam> addParams() {
        LinkedList<NIOParam> params = new LinkedList<>();
        for (Parameter param : taskParams.getParameters()) {
            DataType type = param.getType();
            NIOParam np;
            switch (type) {
                case FILE_T:
                case OBJECT_T:
                case PSCO_T:
                case EXTERNAL_OBJECT_T:
                    DependencyParameter dPar = (DependencyParameter) param;
                    DataAccessId dAccId = dPar.getDataAccessId();
                    Object value = dPar.getDataTarget();
                    boolean preserveSourceData = true;
                    if (dAccId instanceof RAccessId) {
                        // Parameter is a R, has sources
                        preserveSourceData = ((RAccessId) dAccId).isPreserveSourceData();
                    } else if (dAccId instanceof RWAccessId) {
                        // Parameter is a RW, has sources
                        preserveSourceData = ((RWAccessId) dAccId).isPreserveSourceData();
                    } else {
                        // Parameter is a W, it has no sources
                        preserveSourceData = false;
                    }

                    // Workaround for Python PSCOs in return
                    // Check if the parameter has a valid PSCO and change its type
                    String renaming;
                    DataAccessId faId = dPar.getDataAccessId();
                    if (faId instanceof WAccessId) {
                        // Write mode
                        WAccessId waId = (WAccessId) faId;
                        renaming = waId.getWrittenDataInstance().getRenaming();
                    } else if (faId instanceof RWAccessId) {
                        // Read write mode
                        RWAccessId rwaId = (RWAccessId) faId;
                        renaming = rwaId.getWrittenDataInstance().getRenaming();
                    } else {
                        // Read only mode
                        RAccessId raId = (RAccessId) faId;
                        renaming = raId.getReadDataInstance().getRenaming();
                    }
                    String pscoId = Comm.getData(renaming).getId();
                    if (pscoId != null && type.equals(DataType.FILE_T)) {
                        param.setType(DataType.EXTERNAL_OBJECT_T);
                        type = param.getType();
                    }

                    // Create the NIO Param
                    boolean writeFinalValue = !(dAccId instanceof RAccessId); // Only store W and RW
                    np = new NIOParam(type, param.getStream(), param.getPrefix(), preserveSourceData, writeFinalValue, value,
                            (Data) dPar.getDataSource(), dPar.getOriginalName());
                    break;

                default:
                    BasicTypeParameter btParB = (BasicTypeParameter) param;
                    value = btParB.getValue();
                    preserveSourceData = false; // Basic parameters are not preserved on Worker
                    writeFinalValue = false; // Basic parameters are not stored on Worker
                    np = new NIOParam(type, param.getStream(), param.getPrefix(), preserveSourceData, writeFinalValue, value, null,
                            DependencyParameter.NO_NAME);
                    break;
            }

            params.add(np);
        }
        return params;
    }

    public void taskFinished(boolean successful) {
        if (successful) {
            listener.jobCompleted(this);
        } else {
            listener.jobFailed(this, JobEndStatus.EXECUTION_FAILED);
        }
    }

    @Override
    public void stop() throws Exception {
        // Do nothing
    }

    @Override
    public String toString() {
        MethodImplementation method = (MethodImplementation) this.impl;

        String className = method.getDeclaringClass();
        String methodName = taskParams.getName();

        return "NIOJob JobId" + this.jobId + " for method " + methodName + " at class " + className;
    }

}
