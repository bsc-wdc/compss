/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.scheduler.types.ActionGroup.MutexGroup;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.parameter.Parameter;

import java.util.LinkedList;
import java.util.List;


public class CommutativeGroupTask extends AbstractTask {

    private static int commGroupTaskId = -1;
    private final CommutativeIdentifier comId;
    private final DataAccessId accessPlaceholder = new CommutativeDataAccessId();

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
        this.closed = true;
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
    public DataAccessId getGroupPredecessorAccess() {
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

    /**
     * Removes predecessor from group.
     *
     * @param t Predecessor to remove.
     */
    public void removePredecessor(Task t) {
        super.getPredecessors().remove(t);
    }

    public DataAccessId getAccessPlaceHolder() {
        return accessPlaceholder;
    }

    /**
     * Updates the access to perform by the group.
     *
     * @return the access before being updated
     */
    public synchronized DataAccessId nextAccess() {
        DataAccessId oldAccess = this.firstAccess;
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
    public List<Parameter> getParameterDataToRemove() {
        return new LinkedList<>();
    }

    @Override
    public List<Parameter> getIntermediateParameters() {
        return new LinkedList<>();
    }

    @Override
    public List<Parameter> getUnusedIntermediateParameters() {
        return new LinkedList<>();
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
        public DataInstanceId getReadDataInstance() {
            return firstAccess.getReadDataInstance();
        }

        @Override
        public DataInstanceId getWrittenDataInstance() {
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
