package integratedtoolkit.types.job;

import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.MethodImplementation;
import integratedtoolkit.types.implementations.Implementation.TaskType;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.resources.Resource;


/**
 * Representation of a slave job (emtpy job that is released by a masterJob)
 * 
 */
public class SlaveJob extends Job<COMPSsWorker> {

    /**
     * Creates a new Slave Job
     * 
     * @param taskId
     * @param taskParams
     * @param impl
     * @param res
     * @param listener
     */
    public SlaveJob(int taskId, TaskDescription taskParams, Implementation<?> impl, Resource res, JobListener listener) {
        super(taskId, taskParams, impl, res, listener);
    }

    @Override
    public String getHostName() {
        return worker.getName();
    }

    @Override
    public TaskType getType() {
        return TaskType.METHOD;
    }

    @Override
    public String toString() {
        MethodImplementation method = (MethodImplementation) this.impl;

        String className = method.getDeclaringClass();
        String methodName = taskParams.getName();

        return "SlaveJob JobId" + this.jobId + " for method " + methodName + " at class " + className;
    }

    @Override
    public void submit() throws Exception {
        // Log the submission
        logger.info("Submit SlaveJob with ID " + jobId + " to worker " + worker.getName());

        // The listener will be released by the master execution
    }

    @Override
    public void stop() throws Exception {
        // Do nothing
    }

}
