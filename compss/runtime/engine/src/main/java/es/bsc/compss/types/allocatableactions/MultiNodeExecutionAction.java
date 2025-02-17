/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.allocatableactions;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.TaskState;
import es.bsc.compss.worker.COMPSsException;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;


/**
 * Representation of a multi-node execution action.
 */
public class MultiNodeExecutionAction extends ExecutionAction {

    private final MultiNodeGroup group;
    private int actionIdInsideGroup = MultiNodeGroup.ID_UNASSIGNED;


    /**
     * Creates a new master action with a fixed amount of slaves.
     *
     * @param schedulingInformation Scheduling information.
     * @param orchestrator Task orchestrator.
     * @param ap Access Processor.
     * @param task Associated task.
     * @param group Multi-node group.
     */
    public MultiNodeExecutionAction(SchedulingInformation schedulingInformation, ActionOrchestrator orchestrator,
        AccessProcessor ap, Task task, MultiNodeGroup group) {

        super(schedulingInformation, orchestrator, ap, task);

        this.group = group;
    }

    @Override
    public long getGroupPriority() {
        return this.group.isAnyActionRunning() ? ACTION_GROUP_RUNNING : ACTION_GROUP_IDLE;
    }

    /*
     * ***************************************************************************************************************
     * ORCHESTRATOR OPERATIONS
     * ***************************************************************************************************************
     */
    @Override
    protected void notifyCompleted() {
        if (this.actionIdInsideGroup == MultiNodeGroup.ID_MASTER_PROC) {
            if (DEBUG) {
                LOGGER.debug("Notify completed of " + this + " to orchestrator " + this.orchestrator);
            }
            this.group.actionCompletion();
        }
        // Notify orchestrator
        if (isPending()) {
            this.orchestrator.actionCompletion(this);
        }
    }

    @Override
    protected void notifyError() {
        if (this.actionIdInsideGroup == MultiNodeGroup.ID_MASTER_PROC) {

            if (DEBUG) {
                LOGGER.debug("Notify error of " + this + " to orchestrator " + this.orchestrator);
            }
            this.group.actionError();
            this.orchestrator.actionError(this);
        } else {
            if (isRunning()) {
                // Notify orchestrator
                LOGGER.debug("Notify slave " + this + " to orchestrator " + this.orchestrator);
                this.orchestrator.actionError(this);
            } else if (isCancelling()) {
                LOGGER.debug("Notify slave cancelation " + this + " to orchestrator " + this.orchestrator);
                this.orchestrator.actionError(this);
            }
        }
    }

    /*
     * ***************************************************************************************************************
     * EXECUTED SUPPORTING THREAD ON JOB_TRANSFERS_LISTENER
     * ***************************************************************************************************************
     */
    @Override
    protected void doAction() {
        LOGGER.info("Registering action for task " + task.getId());

        this.group.setActionRunning();
        if (this.actionIdInsideGroup == MultiNodeGroup.ID_UNASSIGNED) {
            this.actionIdInsideGroup = this.group.registerProcess(this);
        }

        if (this.actionIdInsideGroup == MultiNodeGroup.ID_MASTER_PROC) {
            // The action is assigned as master, launch as a normal execution
            LOGGER.info("Action registered as master for task " + this.task.getId() + " with groupId "
                + this.group.getGroupId());
            super.doAction();
        } else {
            // The action is assigned as slave, it only waits for task execution
            LOGGER.info("Action registered as slave for task " + this.task.getId() + " with groupId "
                + this.group.getGroupId());
        }
    }

    @Override
    protected List<String> getSlaveNames() {
        return this.group.getSlavesNames();
    }

    /**
     * Code executed to cancel a running execution.
     * 
     * @throws Exception Unstarted node exception.
     */
    @Override
    protected void stopAction() throws Exception {
        // The stop petition needs only to be submitted for the master action
        if (this.actionIdInsideGroup == MultiNodeGroup.ID_MASTER_PROC) {
            LOGGER.info("Task " + this.task.getId() + " starts cancelling MultiNode master running job");
            super.stopAction();
        } else {
            LOGGER.info("Task " + this.task.getId() + " starts cancelling MultiNode slave running job");
        }
    }

    /*
     * ***************************************************************************************************************
     * EXECUTION TRIGGERS
     * ***************************************************************************************************************
     */
    @Override
    protected void doCompleted() {
        if (this.actionIdInsideGroup == MultiNodeGroup.ID_MASTER_PROC) {
            // The action is assigned as master, release all slaves and perform doCompleted as normal task
            super.doCompleted();
        } else {
            // The action is assigned as slave, end profile
            this.getAssignedResource().profiledExecution(this.getAssignedImplementation(), this.profile);
            this.task.decreaseExecutionCount();
        }
    }

    @Override
    protected void doFailed() {
        if (this.actionIdInsideGroup == MultiNodeGroup.ID_MASTER_PROC) {
            // The action is assigned as master, release all slaves and perform doCompleted as normal task
            super.doFailed();
        } else {
            // The action is assigned as slave, mark task as failed
            this.task.setStatus(TaskState.FAILED);
            this.task.decreaseExecutionCount();
        }
    }

    @Override
    protected boolean doCanceled() {
        LOGGER.info("Registering cancel for task " + task.getId());
        this.group.increaseCancelled();
        if (this.group.isCancelled()) {
            return super.doCanceled();
        } else {
            this.task.setStatus(TaskState.CANCELED);
            this.task.decreaseExecutionCount();
            return false;
        }
    }

    @Override

    protected Collection<AllocatableAction> doException(COMPSsException e) {
        if (this.actionIdInsideGroup == MultiNodeGroup.ID_MASTER_PROC) {
            // The action is assigned as master, release all slaves and perform doCompleted as normal task
            return super.doException(e);
        } else {
            // The action is assigned as slave, mark task as failed
            this.task.setStatus(TaskState.FINISHED);
            this.task.decreaseExecutionCount();
            return new LinkedList<>();
        }
    }

    @Override
    protected void doFailIgnored() {
        if (this.actionIdInsideGroup == MultiNodeGroup.ID_MASTER_PROC) {
            // The action is assigned as master, release all slaves and perform doCompleted as normal task
            super.doFailIgnored();
        } else {
            // The action is assigned as slave, mark task as failed
            this.task.setStatus(TaskState.FINISHED);
            this.task.decreaseExecutionCount();
        }
    }

    /*
     * ***************************************************************************************************************
     * ORCHESTRATOR OPERATIONS
     * ***************************************************************************************************************
     */
    @Override
    public String toString() {
        return "MultiNodeExecutionAction (Task " + this.task.getId() + ", CE name "
            + this.task.getTaskDescription().getName() + ") with GroupId = " + this.group.getGroupId();
    }

    public void upgrade() {
        orchestrator.actionUpgrade(this);
    }

}
