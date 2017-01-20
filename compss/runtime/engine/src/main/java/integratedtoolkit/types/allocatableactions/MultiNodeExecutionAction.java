package integratedtoolkit.types.allocatableactions;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.components.impl.TaskProducer;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.scheduler.types.ActionOrchestrator;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.SchedulingInformation;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.Task.TaskState;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.JobStatusListener;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;


/**
 * Representation of a Multi Node execution action
 * 
 * @param <P>
 * @param <T>
 * @param <I>
 */
public class MultiNodeExecutionAction<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends ExecutionAction<P, T, I> {

    // LOGGER
    private static final Logger JOB_LOGGER = LogManager.getLogger(Loggers.JM_COMP);

    private final MultiNodeGroup<P, T, I> group;
    private int multiNodeId;


    /**
     * Creates a new master action with a fixed amount of slaves
     * 
     * @param schedulingInformation
     * @param producer
     * @param task
     * @param numSlaves
     * @param forcedResource
     */
    public MultiNodeExecutionAction(SchedulingInformation<P, T, I> schedulingInformation, ActionOrchestrator<P, T, I> orchestrator,
            TaskProducer producer, Task task, ResourceScheduler<P, T, I> forcedResource, MultiNodeGroup<P, T, I> group) {

        super(schedulingInformation, orchestrator, producer, task, forcedResource);

        this.group = group;
    }

    /*
     * ***************************************************************************************************************
     * ORCHESTRATOR OPERATIONS
     * ***************************************************************************************************************
     */
    @Override
    protected void notifyCompleted() {
        if (this.multiNodeId == MultiNodeGroup.MASTER_GROUP_ID) {
            if (DEBUG) {
                LOGGER.debug("Notify completed of " + this + " to orchestrator " + orchestrator);
            }
            group.actionCompletion();
        }

        // Notify orchestrator
        orchestrator.actionCompletion(this);
    }

    @Override
    protected void notifyError() {
        if (this.multiNodeId == MultiNodeGroup.MASTER_GROUP_ID) {
            if (DEBUG) {
                LOGGER.debug("Notify error of " + this + " to orchestrator " + orchestrator);
            }
            group.actionError();
        }
        // Notify orchestrator
        orchestrator.actionError(this);
    }

    /*
     * ***************************************************************************************************************
     * EXECUTED SUPPORTING THREAD ON JOB_TRANSFERS_LISTENER
     * ***************************************************************************************************************
     */
    @Override
    protected void doAction() {
        JOB_LOGGER.info("Registering action for task " + task.getId());

        this.multiNodeId = group.registerProcess(this);
        executionErrors = 0;

        if (this.multiNodeId == MultiNodeGroup.MASTER_GROUP_ID) {
            // The action is assigned as master, launch as a normal execution
            super.doAction();
        } else {
            // The action is assigned as slave, it only waits for task execution
        }
    }

    protected Job<?> submitJob(int transferGroupId, JobStatusListener<P, T, I> listener) {
        // This part can only be executed by the master action
        if (DEBUG) {
            LOGGER.debug(this.toString() + " starts job creation");
        }
        Worker<T, I> w = selectedResource.getResource();
        List<String> slaveNames = group.getSlavesNames();
        Job<?> job = w.newJob(this.task.getId(), this.task.getTaskDescription(), this.selectedImpl, slaveNames, listener);
        job.setTransferGroupId(transferGroupId);
        job.setHistory(Job.JobHistory.NEW);

        return job;
    }

    /*
     * ***************************************************************************************************************
     * EXECUTION TRIGGERS
     * ***************************************************************************************************************
     */
    @Override
    protected void doCompleted() {
        if (this.multiNodeId == MultiNodeGroup.MASTER_GROUP_ID) {
            // The action is assigned as master, release all slaves and perform doCompleted as normal task
            super.doCompleted();
        } else {
            // The action is assigned as slave, end profile
            selectedResource.profiledExecution(selectedImpl, profile);
            task.decreaseExecutionCount();
        }
    }

    @Override
    protected void doFailed() {
        if (this.multiNodeId == MultiNodeGroup.MASTER_GROUP_ID) {
            // The action is assigned as master, release all slaves and perform doCompleted as normal task
            super.doFailed();
        } else {
            // The action is assigned as slave, mark task as failed
            task.setStatus(TaskState.FAILED);
            task.decreaseExecutionCount();
        }
    }

    /*
     * ***************************************************************************************************************
     * ORCHESTRATOR OPERATIONS
     * ***************************************************************************************************************
     */
    @Override
    public String toString() {
        return "MultiNodeExecutionAction ( Task " + task.getId() + ", CE name " + task.getTaskDescription().getName() + ")";
    }

}
