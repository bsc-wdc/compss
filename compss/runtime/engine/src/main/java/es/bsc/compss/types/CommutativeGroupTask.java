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
package es.bsc.compss.types;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.types.ActionGroup.MutexGroup;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.DataVersion;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CommutativeGroupTask extends AbstractTask {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TP_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static int commGroupTaskId = -1;
    private final CommutativeIdentifier comId;
    private final EngineDataAccessId accessPlaceholder = new CommutativeDataAccessId();

    // Tasks that access the data
    private final List<Task> commutativeTasks;
    private boolean closed = false;

    private AbstractTask groupPredecessor;
    private RWAccessId groupPredecessorAccess;

    // Intermediate Accesses
    private RWAccessId firstAccess;
    private LinkedList<RWAccessId> accesses;

    // Task currently being executed
    private final MutexGroup actions;


    /**
     * Creates a new CommutativeTaskGroup instance.
     *
     * @param app Application.
     * @param comId Commutative group identifier.
     */
    public CommutativeGroupTask(Application app, CommutativeIdentifier comId) {
        super(app, commGroupTaskId--);
        this.commutativeTasks = new LinkedList<>();
        firstAccess = null;
        this.accesses = new LinkedList<>();
        this.comId = comId;
        this.actions = new MutexGroup();
        this.getApplication().onCommutativeGroupCreation(this);
    }

    /**
     * Returns the commutative identifier.
     *
     * @return The commutative identifier.
     */
    public CommutativeIdentifier getCommutativeIdentifier() {
        return this.comId;
    }

    /**
     * Returns whether the group is closed or new tasks can be added.
     * 
     * @return {@literal true}, if the group has been closed; {@literal false}, otherwise.
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Closes the group.
     */
    public void close() {
        if (!this.isClosed()) {
            this.closed = true;
            this.getApplication().onCommutativeGroupClosure(this);
        }
    }

    /**
     * Returns the commutative tasks associated to the group.
     *
     * @return The commutative tasks associated to the group.
     */
    public List<Task> getCommutativeTasks() {
        return this.commutativeTasks;
    }

    /**
     * Adds commutative task to group.
     *
     * @param task Task to add.
     */
    public void addCommutativeTask(Task task) {
        this.commutativeTasks.add(task);
        this.getApplication().onTaskBelongsToCommutativeGroup(task, this);
    }

    /**
     * Notifies the group that a Commutative task has finished.
     * 
     * @param task finished task
     */
    public void finishedCommutativeTask(Task task) {
        this.commutativeTasks.remove(task);
        this.setStatus(TaskState.FINISHED);
        this.removePredecessor(task);
        super.getPredecessors().remove(task);
        if (this.getPredecessors().isEmpty()) {
            this.releaseDataDependents();
            this.notifyListeners();
            if (DEBUG) {
                LOGGER.debug("Group " + this.getId() + " ended execution");
                LOGGER.debug("Data dependents of group " + this.getCommutativeIdentifier() + " released ");
            }
        }
    }

    /**
     * Sets the parent task causing a data dependency.
     *
     * @param predecessor last Task producing the group value
     * @param access first access of the commutative group to the data
     */
    public void setGroupPredecessor(AbstractTask predecessor, RWAccessId access) {
        this.groupPredecessor = predecessor;
        this.groupPredecessorAccess = access;
    }

    /**
     * Returns the parent task causing a data dependency.
     *
     * @return The parent task causing a data dependency.
     */
    public AbstractTask getGroupPredecessor() {
        return this.groupPredecessor;
    }

    /**
     * Return the first access of the commutative group to the data.
     * 
     * @return first access of the commutative group to the data
     */
    public EngineDataAccessId getGroupPredecessorAccess() {
        return this.groupPredecessorAccess;
    }

    /**
     * Registers a subsequent access to the group.
     *
     * @param access data access performed by a task of the commutative group.
     */
    public synchronized void addAccess(RWAccessId access) {
        if (firstAccess == null) {
            firstAccess = access;
        } else {
            this.accesses.add(access);
        }
    }

    public EngineDataAccessId getAccessPlaceHolder() {
        return accessPlaceholder;
    }

    /**
     * Updates the access to perform by the group.
     *
     * @return the access before being updated
     */
    public synchronized EngineDataAccessId nextAccess() {
        EngineDataAccessId oldAccess = this.firstAccess;
        if (!this.accesses.isEmpty()) {
            this.firstAccess = this.accesses.remove();
        } else {
            this.firstAccess = null;
        }
        return oldAccess;
    }

    // ---------------------------------------------------------------
    // -------------------- Scheduling support -----------------------
    // ---------------------------------------------------------------
    /**
     * Returns the group of actions representing the tasks of the group.
     *
     * @return the group of actions that belong to the Commutative task group
     */
    public final MutexGroup getActions() {
        return actions;
    }

    @Override
    public boolean isReduction() {
        return false;
    }

    // ---------------------------------------------------------------
    // ------------------------ Graph handling -----------------------
    // ---------------------------------------------------------------
    @Override
    public String getDotDescription() {
        return null;
    }

    @Override
    public String getLegendDescription() {
        return null;
    }

    @Override
    public String getColor() {
        return null;
    }


    private class CommutativeDataAccessId extends RWAccessId {

        @Override
        public int getDataId() {
            return firstAccess.getDataId();
        }

        public DataInfo getAccessedDataInfo() {
            return firstAccess.getAccessedDataInfo();
        }

        @Override
        public DataVersion getReadDataVersion() {
            return firstAccess.getReadDataVersion();
        }

        @Override
        public DataVersion getWrittenDataVersion() {
            return firstAccess.getWrittenDataVersion();
        }

        @Override
        public Direction getDirection() {
            return firstAccess.getDirection();
        }

        @Override
        public boolean isPreserveSourceData() {
            return firstAccess.isPreserveSourceData();
        }

        @Override
        public boolean isWrite() {
            return firstAccess.isWrite();
        }

        @Override
        public EngineDataInstanceId getReadDataInstance() {
            return firstAccess.getReadDataInstance();
        }

        @Override
        public EngineDataInstanceId getWrittenDataInstance() {
            return firstAccess.getWrittenDataInstance();
        }

        @Override
        public int getRVersionId() {
            return firstAccess.getRVersionId();
        }

        @Override
        public int getWVersionId() {
            return firstAccess.getWVersionId();
        }

        @Override
        public String toString() {
            return "CommutativeAccessId-->" + firstAccess.toString();
        }

    }
}
