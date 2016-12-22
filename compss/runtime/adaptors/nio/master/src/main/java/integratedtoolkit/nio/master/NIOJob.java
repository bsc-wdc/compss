package integratedtoolkit.nio.master;

import java.util.LinkedList;
import java.util.List;

import integratedtoolkit.nio.NIOParam;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.commands.Data;

import integratedtoolkit.types.parameter.BasicTypeParameter;
import integratedtoolkit.types.parameter.DependencyParameter;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.data.DataAccessId;
import integratedtoolkit.types.data.DataAccessId.RAccessId;
import integratedtoolkit.types.data.DataAccessId.RWAccessId;
import integratedtoolkit.types.implementations.AbstractMethodImplementation;
import integratedtoolkit.types.implementations.AbstractMethodImplementation.MethodType;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.MethodImplementation;
import integratedtoolkit.types.implementations.Implementation.TaskType;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.JobListener;
import integratedtoolkit.types.job.JobListener.JobEndStatus;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.annotations.parameter.DataType;


public class NIOJob extends Job<NIOWorkerNode> {
    
    private final List<String> slaveWorkersNodeNames;

    public NIOJob(int taskId, TaskDescription taskParams, Implementation<?> impl, Resource res, 
            List<String> slaveWorkersNodeNames, JobListener listener) {
        
        super(taskId, taskParams, impl, res, listener);
        this.slaveWorkersNodeNames = slaveWorkersNodeNames;
    }

    @Override
    public String getHostName() {
        return worker.getName();
    }

    @Override
    public String toString() {
        MethodImplementation method = (MethodImplementation) this.impl;

        String className = method.getDeclaringClass();
        String methodName = taskParams.getName();

        return "NIOJob JobId" + this.jobId + " for method " + methodName + " at class " + className;
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
            if (methodName == null || methodName.isEmpty()){
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
        NIOTask nt = new NIOTask(lang, 
                                debug,
                                absMethodImpl,
                                hasTarget,
                                hasReturn,
                                params, 
                                numParams, 
                                reqs,
                                this.slaveWorkersNodeNames,
                                this.taskId, 
                                this.taskParams.getId(),
                                this.jobId, 
                                this.history, 
                                this.transferId
                      );

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
                case EXTERNAL_PSCO_T:
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

                    boolean writeFinalValue = !(dAccId instanceof RAccessId); // Only store W and RW
                    np = new NIOParam(type, param.getStream(), preserveSourceData, writeFinalValue, value, (Data) dPar.getDataSource());
                    break;

                default:
                    BasicTypeParameter btParB = (BasicTypeParameter) param;
                    value = btParB.getValue();
                    preserveSourceData = false; // Basic parameters are not preserved on Worker
                    writeFinalValue = false; // Basic parameters are not stored on Worker
                    np = new NIOParam(type, param.getStream(), preserveSourceData, writeFinalValue, value, null);
                    break;
            }

            params.add(np);
        }
        return params;
    }

    @Override
    public TaskType getType() {
        return TaskType.METHOD;
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

}
