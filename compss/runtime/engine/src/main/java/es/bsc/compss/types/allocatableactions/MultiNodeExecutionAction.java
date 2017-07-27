package es.bsc.compss.types.allocatableactions;

import java.util.List;

import es.bsc.compss.components.impl.TaskProducer;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.Task.TaskState;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobStatusListener;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;


/**
 * Representation of a multi-node execution action
 *
 */
public class MultiNodeExecutionAction extends ExecutionAction {

    private final MultiNodeGroup group;
    private int multiNodeId = MultiNodeGroup.UNASSIGNED_ID;


    /**
     * Creates a new master action with a fixed amount of slaves
     *
     * @param schedulingInformation
     * @param orchestrator
     * @param producer
     * @param task
     * @param group
     */
    public MultiNodeExecutionAction(SchedulingInformation schedulingInformation, ActionOrchestrator orchestrator, TaskProducer producer,
            Task task, MultiNodeGroup group) {

        super(schedulingInformation, orchestrator, producer, task);

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
        LOGGER.info("Registering action for task " + task.getId());

        this.multiNodeId = group.registerProcess(this);
        executionErrors = 0;

        if (this.multiNodeId == MultiNodeGroup.MASTER_GROUP_ID) {
            // The action is assigned as master, launch as a normal execution
            LOGGER.info("Action registered as master for task " + task.getId() + " with groupId " + this.multiNodeId);
            super.doAction();
        } else {
            // The action is assigned as slave, it only waits for task execution
            LOGGER.info("Action registered as slave for task " + task.getId() + " with groupId " + this.multiNodeId);
        }
    }

    @Override
    protected Job<?> submitJob(int transferGroupId, JobStatusListener listener) {
        // This part can only be executed by the master action
        if (DEBUG) {
            LOGGER.debug(this.toString() + " starts job creation");
        }
        Worker<? extends WorkerResourceDescription> w = this.getAssignedResource().getResource();
        List<String> slaveNames = group.getSlavesNames();
        Job<?> job = w.newJob(this.task.getId(), this.task.getTaskDescription(), this.getAssignedImplementation(), slaveNames, listener);
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
            this.getAssignedResource().profiledExecution(this.getAssignedImplementation(), profile);
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
        return "MultiNodeExecutionAction ( Task " + task.getId() + ", CE name " + task.getTaskDescription().getName() + ") with GroupId = "
                + this.multiNodeId;
    }

}
