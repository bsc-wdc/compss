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

import es.bsc.compss.log.Loggers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Represents a group of Execution Actions that allow the multi-node execution.
 */
public class MultiNodeGroup {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);

    // Constants and group counters
    public static final int ID_MASTER_PROC = 1;
    public static final int ID_UNASSIGNED = -1;
    private static final AtomicLong NEXT_GROUP_ID = new AtomicLong();

    // Group definition
    private final long groupId;
    private final int groupSize;
    private AtomicInteger nextProcessId;
    private boolean isAnyActionRunning;
    private final Set<MultiNodeExecutionAction> remainingActions;
    private final HashMap<Integer, MultiNodeExecutionAction> registeredSlaves;
    private MultiNodeExecutionAction registeredMaster;
    private int cancelled;


    /**
     * Creates a new group of {@code groupSize} size.
     *
     * @param groupSize Group size.
     */
    public MultiNodeGroup(int groupSize) {
        LOGGER.debug("[MultiNodeGroup] Creating new group of size " + groupSize);
        this.groupId = NEXT_GROUP_ID.getAndIncrement();

        this.groupSize = groupSize;
        this.nextProcessId = new AtomicInteger(groupSize);
        this.isAnyActionRunning = false;
        this.remainingActions = new HashSet<>(groupSize);
        this.registeredSlaves = new HashMap<>();
        this.registeredMaster = null;
        this.cancelled = 0;
    }

    public void addAction(MultiNodeExecutionAction action) {
        remainingActions.add(action);
    }

    /**
     * Returns the group Id.
     * 
     * @return The group Id.
     */
    public long getGroupId() {
        return this.groupId;
    }

    /**
     * Returns the group size.
     *
     * @return The group size.
     */
    public int getGroupSize() {
        return this.groupSize;
    }

    /**
     * Returns whether there is an action running whithin the group or not.
     * 
     * @return {@literal true} if an action within the group is running, {@literal false} otherwise.
     */
    public boolean isAnyActionRunning() {
        return this.isAnyActionRunning;
    }

    /**
     * Marks that the group has an action running.
     */
    public void setActionRunning() {
        this.isAnyActionRunning = true;
    }

    /**
     * Returns whether the group is cancelled or not.
     * 
     * @return {@literal true} if an the group is cancelled, {@literal false} otherwise.
     */
    public boolean isCancelled() {
        return this.cancelled == this.groupSize;
    }

    /**
     * Marks that the group is cancelled.
     */
    public void increaseCancelled() {
        this.cancelled++;
    }

    /**
     * Registers a new process into the group and returns its assigned process Id.
     *
     * @param action ExecutionAction to register into the group.
     * @return The assigned process group Id.
     */
    public int registerProcess(MultiNodeExecutionAction action) {
        int actionId = this.nextProcessId.getAndDecrement();
        remainingActions.remove(action);
        if (actionId == ID_MASTER_PROC) {
            // Register process as master
            LOGGER.debug("[MultiNodeGroup] Register action " + action.getId() + " as master of group " + this.groupId);
            this.registeredMaster = action;
        } else {
            // Register process as slave
            LOGGER.debug("[MultiNodeGroup] Register action " + action.getId() + " as slave of group " + this.groupId);
            this.registeredSlaves.put(actionId, action);

            if (remainingActions.size() == this.groupSize - 1) {
                // First registered task, a change could be done in the othe group scores
                LOGGER.debug("[MultiNodeGroup] Upgrading actions of group " + this.groupId);
                updateRemainingActionScore();
            }
        }

        return actionId;
    }

    private void updateRemainingActionScore() {
        for (MultiNodeExecutionAction action : this.remainingActions) {
            action.upgrade();
        }
    }

    /**
     * Returns the action associated to the master of the group.
     *
     * @return The action associated to the master of the current group.
     */
    public MultiNodeExecutionAction getMasterAction() {
        return this.registeredMaster;
    }

    /**
     * Returns the resources' names of all the slaves registered into the group.
     *
     * @return A list of the resources' names of all the slaves registered into the group.
     */
    public List<String> getSlavesNames() {
        List<String> slavesNames = new ArrayList<>();
        for (Entry<Integer, MultiNodeExecutionAction> slave : this.registeredSlaves.entrySet()) {
            slavesNames.add(slave.getValue().getAssignedResource().getName());
        }

        if (LOGGER.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("[MultiNodeGroup] SlaveNames of group ").append(this).append(" are:");
            for (String slaveName : slavesNames) {
                sb.append(" ").append(slaveName);
            }
            LOGGER.debug(sb.toString());
        }

        return slavesNames;
    }

    /**
     * Triggers an action completion to all the slaves registered into the group.
     */
    public void actionCompletion() {
        LOGGER.debug("[MultiNodeGroup] Notify action completion to all slaves of group " + this);
        for (Entry<Integer, MultiNodeExecutionAction> entry : this.registeredSlaves.entrySet()) {
            entry.getValue().notifyCompleted();
        }
    }

    /**
     * Triggers an action failure to all the slaves registered into the group.
     */
    public void actionError() {
        this.nextProcessId = new AtomicInteger(groupSize);
        this.isAnyActionRunning = false;
        LOGGER.debug("[MultiNodeGroup] Notify action error to all slaves of group " + this);
        for (Entry<Integer, MultiNodeExecutionAction> entry : this.registeredSlaves.entrySet()) {
            entry.getValue().notifyError();
        }
    }

    @Override
    public String toString() {
        return "MultiNodeGroup@" + this.groupId;
    }

}
